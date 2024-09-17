from enum import StrEnum


class DeadLetterHeader(StrEnum):
    MESSAGE = "DEAD_LETTER_MESSAGE"
    DETAIL = "DEAD_LETTER_DETAIL"
