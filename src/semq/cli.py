import json
import datetime as dt
from typing import Dict, Optional, Union

from .settings import (
    get_logger,
    SEMQ_DEFAULT_HELLO_WORLD,
)

from .q import SimpleExternalQueue
from .metastore import PartitionFile

logger = get_logger(name=__name__)


class CLI:

    def __init__(self):
        self.execution_timestamp = dt.datetime.utcnow()

    def hello(self, name: Optional[str] = None) -> str:
        return f"Hello, {name or SEMQ_DEFAULT_HELLO_WORLD}!"

    def setup(
            self,
            name: str,
            metastore_path: Optional[str] = None,
    ):
        queue = SimpleExternalQueue(
            name=name,
            metastore_path=metastore_path
        )
        queue.setup()
        return queue.queue_metastore_path

    def cleanup(self, name: str, everything: bool = False):
        queue = SimpleExternalQueue(
            name=name,
        )
        queue.cleanup(everything=everything)


    def pfile_put(self, name: str):
        fsq = SimpleExternalQueue(name=name)
        partition_file = fsq.partition_file_operation_put()
        return partition_file.filepath

    def pfile_get(self, name: str, wait_seconds: int = -1):
        fsq = SimpleExternalQueue(name=name)
        partition_file = fsq.partition_file_operation_get(wait_seconds=wait_seconds)
        return partition_file.filepath

    def pfiles(
            self,
            name: str,
            metastore_path: Optional[str] = None,
            include_files: bool = False,
    ):
        queue = SimpleExternalQueue(
            name=name,
            metastore_path=metastore_path,
        )
        accum = [] if include_files else None
        youngest, oldest, files, accum = PartitionFile.files_info(path=queue.queue_metastore_path, accum=accum)
        return {
            "queue_metastore_path": queue.queue_metastore_path,
            "pfiles_active_total": files,
            "pfile_active_oldest": oldest,
            "pfile_active_youngest": youngest,
            **(
                {
                    "pfiles": accum,
                } if include_files else {
                }
            )
        }

    def size(
            self,
            name: str,
            metastore_path: Optional[str] = None,
            pfiles_only: bool = False,
            ignore_requests: bool = False
    ):
        fsq = SimpleExternalQueue(
            name=name,
            metastore_path=metastore_path,
        )
        return fsq.size(
            include_items=not pfiles_only,
            ignore_requests=ignore_requests
        )

    def put(self, name: str, item: Union[Dict, str], hashing: bool = False) -> Dict:
        queue = SimpleExternalQueue(name=name, item_hashing=hashing)
        item = item if isinstance(item, str) else json.dumps(item)  # Serialize the item if needed
        return queue.put(item=item, item_hashing=hashing)

    def get(self, name: str, wait_seconds: int = -1, fail: bool = False) -> Dict:
        queue = SimpleExternalQueue(name=name)
        return queue.get(wait_seconds=wait_seconds, fail=fail)
