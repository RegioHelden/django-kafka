from collections.abc import Iterable
from functools import wraps
from typing import Callable, Type


def substitute_error(errors: Iterable[Type[Exception]], substitution: Type[Exception]) -> Callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except tuple(errors) as original_error:
                raise substitution from original_error
        return wrapper
    return decorator
