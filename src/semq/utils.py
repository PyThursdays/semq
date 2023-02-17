import datetime as dt
import os.path
from typing import Optional


from .settings import (
    SEMQ_DEFAULT_PARTITION_FILE_ENDING,
    SEMQ_DEFAULT_TRANSACTION_LOG_PATH,
)


def get_new_partition_filepath(
        file_ending: Optional[str] = None,
        transaction_log_path: Optional[str] = None
) -> str:
    # Default values
    file_ending = file_ending or SEMQ_DEFAULT_PARTITION_FILE_ENDING
    transaction_log_path = transaction_log_path or SEMQ_DEFAULT_TRANSACTION_LOG_PATH
    # Build filename
    filename = str(dt.datetime.utcnow().timestamp()) + file_ending
    return os.path.abspath(os.path.join(transaction_log_path, filename))

