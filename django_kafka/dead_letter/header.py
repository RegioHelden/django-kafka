from django_kafka.header import Header


class DeadLetterHeader(Header):
    MESSAGE = "DEAD_LETTER_MESSAGE"
    DETAIL = "DEAD_LETTER_DETAIL"
