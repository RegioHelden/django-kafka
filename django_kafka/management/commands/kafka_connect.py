import logging
from http import HTTPStatus

from django.core.management import CommandError
from django.core.management.base import BaseCommand
from requests import Response
from requests.exceptions import RetryError

from django_kafka import kafka
from django_kafka.connect.connector import Connector, ConnectorStatus
from django_kafka.exceptions import DjangoKafkaError
from django_kafka.management.commands.errors import substitute_error
from django_kafka.utils import retry

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Publish connectors."

    def add_arguments(self, parser):
        parser.add_argument(
            "connector",
            type=str,
            default=None,
            nargs="?",
            help=(
                "Python path to the connector class(es). "
                "Processes all if not provided.",
            ),
        )
        parser.add_argument(
            "--list",
            action="store_true",
            default=False,
            help="List all connectors.",
        )
        parser.add_argument(
            "--validate",
            action="store_true",
            default=False,
            help="Validate connectors.",
        )
        parser.add_argument(
            "--publish",
            action="store_true",
            default=False,
            help="Create/Update/Delete connectors.",
        )
        parser.add_argument(
            "--check-status",
            action="store_true",
            default=False,
            help=(
                "Check status of connectors. Currently RUNNING status is considered "
                "as success.",
            ),
        )
        parser.add_argument(
            "--ignore-failures",
            action="store_true",
            default=False,
            help="The command wont fail if failures were detected. "
            "By default if any failures were detected the "
            "command exist with error status.",
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connectors: list[str] = []
        self.has_failures = False

    @substitute_error([Exception], CommandError)
    def handle(self, connector, **options):
        if options["list"]:
            self.list_connectors()
            return

        if not any(
            (
                connector,
                options["validate"],
                options["publish"],
                options["check_status"],
            ),
        ):
            self.print_help("manage.py", "kafka_connect")
            return

        if connector:
            self.connectors = [connector]
        else:
            self.connectors = kafka.connectors

        if options["validate"]:
            self.handle_validate()

        if options["publish"]:
            self.handle_publish()

        if options["check_status"]:
            self.handle_status()

        if self.has_failures and not options["ignore_failures"]:
            raise CommandError("Command does not succeed.")

    def list_connectors(self):
        self.stdout.write(self.style.SUCCESS("Available connectors:"))
        for connector_path in kafka.connectors:
            if kafka.connectors[connector_path].mark_for_removal:
                self.stdout.write(f"- {connector_path} (marked for removal)")
            else:
                self.stdout.write(f"- {connector_path}")

    def handle_validate(self):
        self.stdout.write(self.style.SUCCESS("Validating connectors..."))

        for connector_path in self.connectors:
            self.stdout.write(f"{connector_path}: ", ending="")

            connector = kafka.connectors[connector_path]()

            if connector.mark_for_removal:
                self.stdout.write(
                    self.style.WARNING("skip (REASON: marked for removal)"),
                )
                continue

            try:
                connector.is_valid(raise_exception=True)
            except (DjangoKafkaError, RetryError) as error:
                self.has_failures = True
                self.stdout.write(self.style.ERROR("invalid"))
                self.stdout.write(self.style.ERROR(str(error)))
            else:
                self.stdout.write(self.style.SUCCESS("valid"))

    def handle_publish(self):
        self.stdout.write(self.style.SUCCESS("Publishing connectors..."))

        for connector_path in self.connectors:
            self.stdout.write(f"{connector_path}: ", ending="")

            connector = kafka.connectors[connector_path]()

            if connector.mark_for_removal:
                self.handle_delete(connector)
            else:
                self.handle_submit(connector)

    def handle_status(self):
        self.stdout.write(self.style.SUCCESS("Checking status..."))

        for connector_path in self.connectors:
            self.stdout.write(f"{connector_path}: ", ending="")

            connector = kafka.connectors[connector_path]()
            try:
                self._connector_is_running(connector)
            except DjangoKafkaError as error:
                self.has_failures = True
                self.stdout.write(self.style.ERROR(str(error)))

    @retry((DjangoKafkaError,), tries=3, delay=1, backoff=2)
    def _connector_is_running(self, connector: Connector):
        if connector.mark_for_removal:
            self.stdout.write(self.style.WARNING("skip (REASON: marked for removal)"))
            return
        try:
            status = connector.status()
        except DjangoKafkaError as error:
            if (
                isinstance(error.context, Response)
                and error.context.status_code == HTTPStatus.NOT_FOUND
            ):
                # retry: on 404 as some delays are expected
                raise

            self.has_failures = True
            self.stdout.write(self.style.ERROR("failed to retrieve"))
            self.stdout.write(self.style.ERROR(str(error)))
        except RetryError as error:
            self.has_failures = True
            self.stdout.write(self.style.ERROR("failed to retrieve"))
            self.stdout.write(self.style.ERROR(str(error)))
        else:
            if status == ConnectorStatus.RUNNING:
                self.stdout.write(self.style.SUCCESS(status))
            elif status == ConnectorStatus.UNASSIGNED:
                # retry: on UNASSIGNED as some delays are expected
                raise DjangoKafkaError(f"Connector status is {status}.")
            else:
                self.has_failures = True
                self.stdout.write(self.style.ERROR(status))

    def handle_delete(self, connector: Connector):
        try:
            deleted = connector.delete()
        except (DjangoKafkaError, RetryError) as error:
            self.has_failures = True
            self.stdout.write(self.style.ERROR("failed"))
            self.stdout.write(self.style.ERROR(str(error)))
        else:
            if deleted:
                self.stdout.write(self.style.SUCCESS("deleted"))
            else:
                self.stdout.write(
                    self.style.WARNING("does not exist (already deleted)"),
                )

    def handle_submit(self, connector: Connector):
        try:
            connector.submit()
        except (DjangoKafkaError, RetryError) as error:
            self.has_failures = True
            self.stdout.write(self.style.ERROR("failed"))
            self.stdout.write(self.style.ERROR(str(error)))
        else:
            self.stdout.write(self.style.SUCCESS("submitted"))
