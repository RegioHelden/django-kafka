import logging

from django.core.management.base import BaseCommand

from django_kafka import kafka
from django_kafka.consumer.runner import KafkaConsumeRunner

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Start python consumers in parallel."

    def add_arguments(self, parser):
        parser.add_argument(
            "consumers",
            nargs="*",
            type=str,
            default=list(kafka.consumers),
            help="Python path to the consumer class(es). Starts all if not provided.",
        )

    def handle(self, consumers: list[str], *args, **options):
        KafkaConsumeRunner(consumers).start()
