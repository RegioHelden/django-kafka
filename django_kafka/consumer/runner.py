import contextlib
import logging
import signal
import time
from multiprocessing import Event, Process

import django
from django import db
from django.apps import apps

from django_kafka import kafka
from django_kafka.exceptions import DjangoKafkaError

logger = logging.getLogger(__name__)


class KafkaConsumeRunner:
    def __init__(self, consumers: list[str]):
        self.consumers = consumers
        self.processes: list[Process] = []
        self.stop_event: Event = Event()

    def start(self):
        signal.signal(signal.SIGINT, self._soft_shutdown)  # Ctrl+C or kill -SIGINT
        signal.signal(signal.SIGTERM, self._soft_shutdown)  # kill <pid>

        db.connections.close_all()  # close before spawning new processes

        try:
            # spawn one process per consumer
            for key in self.consumers:
                worker = ConsumerWorker(key, self.stop_event)
                process = Process(target=worker.start, name=f"consumer-{key}")
                process.start()
                self.processes.append(process)

            # waiting loop
            while any(p.exitcode is None for p in self.processes):
                if any(p.exitcode not in (None, 0) for p in self.processes):
                    # shut down if any has failed.
                    self._soft_shutdown(None, None)
                    raise DjangoKafkaError(
                        "The consumer runner process exited unexpectedly.",
                    )
                time.sleep(0.2)

        finally:
            for process in self.processes:
                # timeout is to avoid infinite wait, if anything goes wrong
                process.join(timeout=1.0)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def _soft_shutdown(self, signum, frame):
        logger.info("Soft shutdown - waiting for consumers to finish")
        if signum == signal.SIGINT:
            logger.warning(
                "Hitting Ctrl+C again will kill running processes immediately!",
            )
        self.stop_event.set()
        if signal.getsignal(signal.SIGINT) is not self._hard_shutdown:
            signal.signal(signal.SIGINT, self._hard_shutdown)

    def _hard_shutdown(self, signum, frame):
        logger.info("Hard shutdown - immediately stopping consumers")

        for process in self.processes:
            if process.is_alive():
                with contextlib.suppress(Exception):
                    process.terminate()

        # little waiting loop, to cover delays in terminations
        end = time.time() + 2  # loop for 2 seconds maximum
        while time.time() < end and any(p.is_alive() for p in self.processes):
            time.sleep(0.05)

        # if someone is still alive - hard kill
        for process in self.processes:
            if process.is_alive():
                process.kill()
        # after hard shutdown, make further SIGINT reset to default
        signal.signal(signal.SIGINT, signal.SIG_DFL)


class ConsumerWorker:
    """
    Consumer worker started by subprocesses, so all internal state must be pickleable.
    """

    def __init__(self, consumer_key: str, stop_event: Event):
        self.consumer_key = consumer_key
        self.stop_event = stop_event

    def start(self):
        # Only parent handles Ctrl+C (SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        # Ensure SIGTERM in the child uses the default action (terminate)
        # to avoids inheriting the parent's _shutdown handler
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        if not apps.ready:
            django.setup()  # for spawn or forkserver process start methods

        try:
            kafka.consumers[self.consumer_key]().start(self.stop_event)
        except Exception:
            logger.exception("Consumer %s crashed", self.consumer_key)
            raise
