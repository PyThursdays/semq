import os
import uuid
import json
import shutil
import datetime as dt
from typing import Dict, List, Tuple, Optional

from .metastore import FilePrefix, PartitionFile, RequestFile
from .exceptions import (
    UnavailablePartitionFiles,
    RequestIdentifierNotFoundInRequestFile,
)
from .settings import (
    get_logger,
    SEMQ_DEFAULT_METASTORE_PATH,
    SEMQ_DEFAULT_PARTITION_SIZE,
    SEMQ_DEFAULT_METASTORE_TRASHDIR,
)


logger = get_logger(name=__name__)


class SimpleExternalQueue:

    def __init__(
            self,
            name: str,
            metastore_path: Optional[str] = None,
            partition_file_size: Optional[int] = None,
            item_hashing: bool = False,
            trash_dirname: Optional[str] = None,
    ):
        self.name = name
        self.metastore_path = metastore_path or SEMQ_DEFAULT_METASTORE_PATH
        self.queue_metastore_path = os.path.join(self.metastore_path, self.name)
        self.partition_file_size = partition_file_size or SEMQ_DEFAULT_PARTITION_SIZE
        self.item_hashing = item_hashing
        self.trash_dirname = trash_dirname or SEMQ_DEFAULT_METASTORE_TRASHDIR
        self.trash_dirpath = os.path.join(self.queue_metastore_path, self.trash_dirname)

    def setup(self):
        # Create the metastore path if not exists
        os.makedirs(self.queue_metastore_path, exist_ok=True)
        os.makedirs(self.trash_dirpath, exist_ok=True)

    def cleanup(self, everything: bool = False):
        shutil.rmtree(self.trash_dirpath)
        if everything:
            shutil.rmtree(self.queue_metastore_path)
        self.setup()

    @classmethod
    def discover(cls, metastore_path: Optional[str] = None) -> List[Dict]:
        metastore_path = metastore_path or SEMQ_DEFAULT_METASTORE_PATH

        return [
            {
                "name": queue_name,
                "path": queue_path,
                "created_at": os.path.getctime(queue_path),
                "updated_at": os.path.getmtime(queue_path),

            }
            for queue_name in os.listdir(metastore_path)
            for queue_path in [os.path.join(metastore_path, queue_name)]
        ]

    def partition_file_operation_put(self, item_hashing: bool = False) -> PartitionFile:
        return PartitionFile.from_path_mode_put(
            max_size=self.partition_file_size,
            path=self.queue_metastore_path,
            item_hashing=item_hashing,
        )

    def partition_file_operation_get(
            self,
            wait_seconds: int = -1,
    ) -> PartitionFile:
        return PartitionFile.from_path_mode_get(
            max_size=self.partition_file_size,
            path=self.queue_metastore_path,
            wait_seconds=wait_seconds,
        )

    def put(self, item: str, item_hashing: bool = False) -> Dict:
        partition_file = self.partition_file_operation_put(item_hashing=item_hashing)
        payload, _ = partition_file.append(item=item)
        return payload

    def get_request(
            self,
            wait_seconds: int = -1,
    ) -> Tuple[RequestFile, str]:
        request_id = str(uuid.uuid4())
        request_file = self.partition_file_operation_get(wait_seconds=wait_seconds).get_request_file(
            trash_dirpath=self.trash_dirpath
        )
        return request_file.request(request_id=request_id, wait_seconds=wait_seconds), request_id

    def get(
            self,
            wait_seconds: int = -1,
            fail: bool = False,
            exclude_metadata: bool = False,
    ) -> Optional[Dict]:
        try:
            request_file, request_id = self.get_request(wait_seconds=wait_seconds)
            with open(request_file.filepath, "r") as rfile:
                for i, line in enumerate(rfile):
                    if line.startswith(request_id):
                        break
                else:
                    # Request ID not found in request file
                    raise RequestIdentifierNotFoundInRequestFile(
                        req_id=request_id,
                        req_file=request_file.filepath,
                    )

            # Found request position; extracting same position from partition file
            with open(request_file.partition_file.filepath, "r") as pfile:
                for queue_position, line in enumerate(pfile):
                    if queue_position == i:
                        payload = json.loads(line.strip())
                        if exclude_metadata:
                            return payload.get("item")
                        payload["item_request_id"] = request_id
                        payload["item_request_file"] = request_file.filepath
                        payload["item_retrieved_at"] = dt.datetime.utcnow().isoformat()
                        return payload
        except UnavailablePartitionFiles:
            if fail:
                raise
            return

    def is_empty(self) -> bool:
        _, _, files, _ = PartitionFile.files_info(path=self.queue_metastore_path)
        return files == 0

    def size(self, include_items: bool = False, ignore_requests: bool = False):
        payload = {
            "timestamp": dt.datetime.utcnow().isoformat(),
        }
        files = [] if include_items else None
        _, _, num_files, file_names = PartitionFile.files_info(
            path=self.queue_metastore_path,
            accum=files
        )
        payload["active_partition_files"] = num_files
        logger.info("Size of active partition files: %d", num_files)
        if not include_items:
            return payload
        items = 0
        requests = 0
        for file_name in files:
            file_path = os.path.join(self.queue_metastore_path, file_name)
            with open(file_path, "r") as pfile:
                for i, _ in enumerate(pfile):
                    pass
            items += i + 1
            if ignore_requests:
                continue
            request_file = FilePrefix.apply_prefix_request(file_path)
            if os.path.exists(request_file):
                with open(request_file, "r") as rfile:
                    for i, _ in enumerate(rfile):
                        pass
                requests += i + 1
        return {
            **payload,
            "total_pending_items": items - requests,
            "total_items_in_pfiles": items,
            "total_requests_in_rfiles": requests,
        }
