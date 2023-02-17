import logging
import os

SEMQ_DEFAULT_HELLO_WORLD = os.environ.get(
    "SEMQ_DEFAULT_HELLO_WORLD",
    default="world",
)


def get_logger(name: str):
    return logging.getLogger(name)
