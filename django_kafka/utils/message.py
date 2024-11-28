from typing import Optional

MessageHeaders = list[tuple[str, str]]


class Header:
    @classmethod
    def list(cls, headers: Optional[MessageHeaders], header: str) -> list[str]:
        """returns all occurrences for the header"""
        if headers is not None:
            return [v for k, v in headers if k == header]
        return []

    @classmethod
    def get(cls, headers: Optional[MessageHeaders], header: str) -> Optional[str]:
        """returns the first encountered value for the header"""
        if headers is not None:
            return next((v for k, v in headers if k == header), None)
        return None
