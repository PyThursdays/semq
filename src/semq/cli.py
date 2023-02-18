import json
import datetime as dt
from typing import Dict, Optional

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

    def pfile_put(self):
        fsq = FileSystemQueue()
        return fsq.partition_file_operation_put.filepath

    def pfile_get(self):
        fsq = FileSystemQueue()
        return fsq.partition_file_operation_get.filepath

    def put(self, **kwargs) -> str:
        fsq = FileSystemQueue()
        print(fsq.partition_file_size)
        item = json.dumps(kwargs)
        fsq.put(item=item)
        return fsq.partition_file_operation_put.filepath

    def get(self) -> Dict:
        fsq = FileSystemQueue()
        return fsq.get()
