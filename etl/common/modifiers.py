import time
from functools import wraps


def backoff(start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10):
    """
    Re-executes the function after a potential fail while not succeed
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            logger = getattr(args[0], "logger") if hasattr(args[0], "logger") else None
            sleep_time = start_sleep_time
            n = 0
            while True:
                n += 1
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if logger:
                        logger.error(f"An error in executing {func.__name__}. Retrying in {sleep_time} sec.")
                    time.sleep(sleep_time)
                    new_delta = start_sleep_time * (factor ** n)
                    sleep_time = new_delta if new_delta < border_sleep_time else border_sleep_time
        return inner
    return func_wrapper


class Singleton(type):
    """
    Singleton realisation.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
