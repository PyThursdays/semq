import json
import datetime as dt
from typing import Optional

from .settings import (
    get_logger,
    SEMQ_DEFAULT_HELLO_WORLD,
)

from .utils import get_new_partition_filepath
from .models import FileSystemQueue

logger = get_logger(name=__name__)


class CLI:

    def __init__(self):
        self.execution_timestamp = dt.datetime.utcnow()

    def hello(self, name: Optional[str] = None) -> str:
        return f"Hello, {name or SEMQ_DEFAULT_HELLO_WORLD}!"

    def filepath_example(self, **kwargs):
        return get_new_partition_filepath(**kwargs)

    def put(self, **kwargs) -> str:
        fsq = FileSystemQueue()
        print(fsq.partition_file_size)
        item = json.dumps(kwargs)
        fsq.put(item=item)
        return fsq.partition_file.filepath
