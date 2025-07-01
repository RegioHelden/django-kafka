from pydoc import locate
from typing import TYPE_CHECKING

from django_kafka.conf import settings
from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.relations_resolver.processor.base import MessageProcessor


def get_message_processor() -> "MessageProcessor":
    if not (processor_cls := locate(settings.RELATION_RESOLVER_PROCESSOR)):
        raise DjangoKafkaError(f"{settings.RELATION_RESOLVER_PROCESSOR} not found.")
    return processor_cls()
