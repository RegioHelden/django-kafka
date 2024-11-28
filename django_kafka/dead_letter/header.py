from django_kafka.utils.message import Header


class DeadLetterHeader(Header):
    MESSAGE = "DEAD_LETTER_MESSAGE"
    DETAIL = "DEAD_LETTER_DETAIL"
