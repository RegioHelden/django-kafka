import logging

from django.core.management.base import BaseCommand

from django_kafka import kafka

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Start python consumers in parallel."

    def add_arguments(self, parser):
        parser.add_argument(
            "consumers",
            nargs="*",
            type=str,
            default=None,
            help="Python path to the consumer class(es). Starts all if not provided.",
        )

    def handle(self, consumers: list[str] | None = None, *args, **options):
        kafka.run_consumers(consumers)
