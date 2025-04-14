from abc import ABC

from django.db.models import Model

from django_kafka.exceptions import DjangoKafkaError
from django_kafka.topic.model import ModelTopicConsumer


class DbzModelTopicConsumer(ModelTopicConsumer, ABC):
    """Syncs a debezium source connector topic directly in to Django model instances"""

    reroute_key_field_name = "__dbz__physicalTableIdentifier"  # see: https://debezium.io/documentation/reference/stable/transformations/topic-routing.html#by-logical-table-router-key-field-name
    reroute_model_map: dict[str, type[Model]] | None = None

    def get_model(self, key, value) -> type[Model]:
        if self.reroute_key_field_name in key:
            if not self.reroute_model_map:
                raise DjangoKafkaError(
                    f"To obtain the correct model, reroute_model_map must be set when "
                    f"`{self.reroute_key_field_name}` is present in the message key",
                )
            table = key[self.reroute_key_field_name]
            if table not in self.reroute_model_map:
                raise DjangoKafkaError(f"Unrecognised rerouted topic `{table}`")
            return self.reroute_model_map[table]
        return super().get_model(key, value)

    def is_deletion(self, model, key, value) -> bool:
        if value is None:
            return True

        deleted = value.pop("__deleted", None)
        if isinstance(deleted, bool):
            return deleted
        if isinstance(deleted, str):
            return deleted.lower() == "true"
        return False

    def get_lookup_kwargs(self, model, key, value) -> dict:
        pk_field = model._meta.pk.name
        return {pk_field: key[pk_field]}
