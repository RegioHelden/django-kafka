from unittest.mock import Mock


def message_mock(topic="topic", partition=0, offset=0, error=None, headers=None):
    """mocking utility for confluent_kafka.cimpl.Message"""
    return Mock(
        **{
            "topic.return_value": topic,
            "partition.return_value": partition,
            "offset.return_value": offset,
            "headers.return_value": headers,
            "error.return_value": error,
        },
    )
