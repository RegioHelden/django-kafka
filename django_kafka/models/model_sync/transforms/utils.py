from __future__ import annotations

from enum import Flag, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class MessagePart(Flag):
    KEY = auto()
    VALUE = auto()
    BOTH = KEY | VALUE
