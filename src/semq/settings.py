import os
import logging
from typing import Optional

SEMQ_DEFAULT_HELLO_WORLD = os.environ.get(
    "SEMQ_DEFAULT_HELLO_WORLD",
    default="world",
)

SEMQ_DEFAULT_METASTORE_PATH = os.environ.get(
    "SEMQ_DEFAULT_METASTORE_PATH",
    default=os.path.abspath(os.path.join(".", "metastore"))
)

SEMQ_DEFAULT_METASTORE_TRASHDIR = os.environ.get(
    "SEMQ_DEFAULT_METASTORE_TRASHDIR",
    default=".trash",
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

SEMQ_DEFAULT_LOGGING_LEVEL = os.environ.get(
    "SEMQ_DEFAULT_LOGGING_LEVEL",
    default="INFO",
)


def get_logger(name: str, log_level: Optional[str] = None):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(log_level or SEMQ_DEFAULT_LOGGING_LEVEL)
    return logger
