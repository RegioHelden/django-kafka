import time
from functools import wraps
from typing import Type


def retry(
    exceptions: tuple[Type[Exception]] = (Exception,),
    tries: int = -1,
    delay: int = 0,
    backoff: int = 1,
):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            _tries = tries
            _delay = delay

            while _tries:
                try:
                    return f(*args, **kwargs)
                except exceptions:
                    _tries -= 1
                    if not _tries:
                        raise
                    time.sleep(_delay)
                    _delay *= backoff
        return wrapper
    return decorator
