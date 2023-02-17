import logging
import os

SEMQ_DEFAULT_HELLO_WORLD = os.environ.get(
    "SEMQ_DEFAULT_HELLO_WORLD",
    default="world",
)

SEMQ_DEFAULT_TRANSACTION_LOG_PATH = os.environ.get(
    "SEMQ_DEFAULT_TRANSACTION_LOG_PATH",
    default="."
)

SEMQ_DEFAULT_PARTITION_SIZE = int(os.environ.get(
    "SEMQ_DEFAULT_PARTITION_SIZE",
    default=3,
))


SEMQ_DEFAULT_PARTITION_FILE_ENDING = os.environ.get(
    "SEMQ_DEFAULT_PARTITION_FILE_ENDING",
    default=".json"
)

if not SEMQ_DEFAULT_PARTITION_FILE_ENDING.startswith("."):
    SEMQ_DEFAULT_PARTITION_FILE_ENDING = "." + SEMQ_DEFAULT_PARTITION_FILE_ENDING


def get_logger(name: str):
    return logging.getLogger(name)
