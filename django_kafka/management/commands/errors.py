from collections.abc import Callable, Iterable
from functools import wraps


def substitute_error(
    errors: Iterable[type[Exception]],
    substitution: type[Exception],
) -> Callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except tuple(errors) as original_error:
                raise substitution(original_error) from original_error

        return wrapper

    return decorator
