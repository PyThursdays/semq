import os
import uuid
import json
import enum
import time
import datetime as dt
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

from .utils import get_new_partition_filepath
from .exceptions import (
    UnavailablePartitionFiles,
    RequestIdentifierNotFoundInRequestFile,
)
from .settings import (
    get_logger,
    SEMQ_DEFAULT_METASTORE_PATH,
    SEMQ_DEFAULT_PARTITION_SIZE
)


logger = get_logger(name=__name__)


class FilePrefix(enum.Enum):
    REQ = "req"
    DEL = "del"

    @classmethod
    def apply_prefix_delete(cls, filepath: str) -> str:
        directory, file = os.path.dirname(filepath), os.path.basename(filepath)
        return os.path.join(
            directory,
            f"{cls.DEL.value}-{file}",
        )

    @classmethod
    def apply_prefix_request(cls, filepath: str) -> str:
        directory, file = os.path.dirname(filepath), os.path.basename(filepath)
        return os.path.join(
            directory,
            f"{cls.REQ.value}-{file}",
        )


class AbstractFile:
    
    def __init__(self, filepath: str):
        self.filepath = filepath

    @property
    def size(self):
        if not os.path.exists(self.filepath):
            return 0
        with open(self.filepath, "r") as file:
            for i, _ in enumerate(file):
                continue
            return i + 1

    def create_if_not_exists(self):
        if not os.path.exists(self.filepath):
            open(self.filepath, "x").close()
        return self

    def soft_delete(self) -> bool:
        return os.rename(
            self.filepath,
            FilePrefix.apply_prefix_delete(filepath=self.filepath)
        ) or True


@dataclass
class RequestFile(AbstractFile):
    filepath: str
    partition_file: 'PartitionFile'

    def refresh(self, wait_seconds: int = -1) -> 'RequestFile':
        partition_file_configs = {
            "max_size": self.partition_file.max_size,
            "path": os.path.dirname(self.partition_file.filepath),
            "wait_seconds": wait_seconds,
        }
        self.partition_file.soft_delete(), self.soft_delete()
        partition_file = self.partition_file.from_path_mode_get(**partition_file_configs)
        return partition_file.get_request_file()

    def request(self, request_id: str, wait_seconds: int = -1):
        partition_file_size = self.partition_file.size

        with open(self.filepath, "r+") as file:
            for i, _ in enumerate(file):
                if i + 1 >= partition_file_size:
                    return self.refresh(wait_seconds=wait_seconds).request(
                        request_id=request_id,
                        wait_seconds=wait_seconds,
                    )
            file.write(request_id + "\n")
        return self


@dataclass
class PartitionFile(AbstractFile):
    filepath: str
    max_size: int
    partition_files: Optional[int] = None
    item_hashing: bool = False

    class Mode(enum.Enum):
        PUT = 1
        GET = -1

    @classmethod
    def from_path_mode_put(
            cls,
            path: str,
            max_size: int,
            item_hashing: bool = False,
    ):
        return cls.from_path(
            mode=cls.Mode.PUT,
            max_size=max_size,
            path=path,
            # PUT Config
            item_hashing=item_hashing,
        )

    @classmethod
    def from_path_mode_get(
            cls,
            path: str,
            max_size: int,
            wait_seconds: int = -1,
    ):
        return cls.from_path(
            mode=cls.Mode.GET,
            max_size=max_size,
            path=path,
            # GET Config
            wait_seconds=wait_seconds,
        )

    @staticmethod
    def files_info(path: str, accum: Optional[List] = None) -> Tuple[str, str, int, Optional[List]]:
        # Prefix to ignore
        prefix_options = tuple(prefix.value for prefix in FilePrefix)
        # Define the start and final values to compare with youngest or oldest
        youngest, oldest = "0000-00-00.000000.json", "9999-99-99.999999.json"
        # Initialize file counter to zero.
        files = 0
        accumulate = accum is not None
        # Start scanning
        logger.info("Scanning Path for partition files.")
        for file in os.listdir(path):
            if file.startswith(prefix_options):
                continue
            logger.debug("> Partition file iter %d", files)
            # Find the oldest file for "get" scenario
            oldest = oldest if oldest < file else file
            # Find the youngest file for "put" scenario
            youngest = youngest if youngest > file else file
            files += 1
            if accumulate:
                accum.append(file)
        return youngest, oldest, files, accum

    @classmethod
    def from_path(
            cls,
            path: str,
            mode: Mode,
            max_size: int,
            item_hashing: bool = False,
            wait_seconds: int = -1,
    ):
        youngest, oldest, files, _ = cls.files_info(path=path, accum=None)
        if not files and mode == cls.Mode.GET:
            logger.warning("Partition files not found in GET request")
            if wait_seconds < 1:
                raise UnavailablePartitionFiles(path=path)
            time.sleep(wait_seconds)
            return cls.from_path(
                wait_seconds=wait_seconds,
                item_hashing=item_hashing,
                max_size=max_size,
                mode=mode,
                path=path,
            )
        reference = oldest if mode == cls.Mode.GET else youngest if mode == cls.Mode.PUT else None
        logger.debug("Reference partition file set to: %s", reference)
        return cls(
            filepath=(
                get_new_partition_filepath(file_path=path)
                if not files else
                os.path.join(path, reference)
            ),
            max_size=max_size,
            partition_files=files,
            item_hashing=item_hashing,
        ).create_if_not_exists()

    def get_request_file(self) -> RequestFile:
        return RequestFile(
            filepath=FilePrefix.apply_prefix_request(filepath=self.filepath),
            partition_file=self,
        ).create_if_not_exists()

    @classmethod
    def new(cls, max_size: int, partition_files: Optional[int] = None):
        return cls(
            filepath=get_new_partition_filepath(),
            max_size=max_size,
            partition_files=partition_files,
        ).create_if_not_exists()

    def append(self, item: str) -> Tuple[Dict, 'PartitionFile']:
        # Create the newline content
        payload = {
            "partition_filepath": self.filepath,
            "item_created_at": dt.datetime.utcnow().isoformat(),
            "item_id": str(
                uuid.uuid4() if not self.item_hashing else uuid.uuid5(
                    uuid.NAMESPACE_OID,
                    item,
                )
            ),
            "item": item,
        }
        line = json.dumps(payload) + "\n"
        with open(self.filepath, "r+") as file:
            for i, _ in enumerate(file):
                # Soft max validation; should we add a new line to current file or create a new one?
                if i + 1 >= self.max_size:
                    return PartitionFile.new(max_size=self.max_size).append(item=item)
            file.write(line)
        return payload, self


class FileSystemQueue:

    def __init__(
            self,
            name: str,
            metastore_path: Optional[str] = None,
            partition_file_size: Optional[int] = None,
            item_hashing: bool = False,
    ):
        self.name = name
        self.metastore_path = metastore_path or SEMQ_DEFAULT_METASTORE_PATH
        self.queue_metastore_path = os.path.join(self.metastore_path, self.name)
        self.partition_file_size = partition_file_size or SEMQ_DEFAULT_PARTITION_SIZE
        self.item_hashing = item_hashing
        # Create the metastore path if not exists
        os.makedirs(self.queue_metastore_path, exist_ok=True)

    def partition_file_operation_put(self, item_hashing: bool = False) -> PartitionFile:
        return PartitionFile.from_path_mode_put(
            max_size=self.partition_file_size,
            path=self.metastore_path,
            item_hashing=item_hashing,
        )

    def partition_file_operation_get(
            self,
            wait_seconds: int = -1,
    ) -> PartitionFile:
        return PartitionFile.from_path_mode_get(
            max_size=self.partition_file_size,
            path=self.metastore_path,
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
        request_file = self.partition_file_operation_get(wait_seconds=wait_seconds).get_request_file()
        return request_file.request(request_id=request_id, wait_seconds=wait_seconds), request_id

    def get(
            self,
            wait_seconds: int = -1,
            fail: bool = False,
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
