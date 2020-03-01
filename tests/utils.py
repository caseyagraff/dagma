from functools import wraps


class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

    def reset(self):
        self.count = 0

    def __repr__(self):
        return f"Counter({self.count})"


def call_count(counter):
    """
    A decorator (with a mandatory argument counter) that counts the number of times the
    wrapped function has been called.
    """

    def create(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            counter.increment()
            return func(*args, **kwargs)

        return wrapper

    return create
