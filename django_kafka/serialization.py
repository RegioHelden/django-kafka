from confluent_kafka.serialization import Serializer


class NoOpSerializer(Serializer):
    """performs no messages serialization, returning the message without modification"""

    def __call__(self, obj, ctx=None):
        return obj
