import datetime as dt
import os.path
from typing import Optional


from .settings import (
    SEMQ_DEFAULT_PARTITION_FILE_ENDING,
)


def get_new_partition_filepath(
        file_path: str,
        file_ending: Optional[str] = None,
) -> str:
    # Default values
    file_ending = file_ending or SEMQ_DEFAULT_PARTITION_FILE_ENDING
    # Build filename
    filename = str(dt.datetime.utcnow().timestamp()) + file_ending
    return os.path.abspath(os.path.join(file_path, filename))
