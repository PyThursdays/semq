import os
import uuid
import json
import enum
import datetime as dt
from typing import Dict, Tuple, Optional, Union
from dataclasses import dataclass

from .utils import get_new_partition_filepath
from .settings import (
    SEMQ_DEFAULT_TRANSACTION_LOG_PATH,
    SEMQ_DEFAULT_PARTITION_SIZE
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
            os.path.join(
                os.path.dirname(self.filepath),
                f"del-{os.path.basename(self.filepath)}",
            )
        ) or True


@dataclass
class RequestFile(AbstractFile):
    filepath: str
    partition_file: 'PartitionFile'

    def refresh(self) -> 'RequestFile':
        partition_file_configs = {
            "max_size": self.partition_file.max_size,
            "path": os.path.dirname(self.partition_file.filepath),
        }
        self.partition_file.soft_delete(), self.soft_delete()
        partition_file = self.partition_file.from_path_mode_get(**partition_file_configs)
        print(partition_file.filepath)
        return partition_file.get_request_file()

    def request(self, request_id: str):
        partition_file_size = self.partition_file.size

        with open(self.filepath, "r+") as file:
            for i, _ in enumerate(file):
                if i + 1 >= partition_file_size:
                    return self.refresh().request(request_id=request_id)
            file.write(request_id + "\n")
        return self


@dataclass
class PartitionFile(AbstractFile):
    filepath: str
    max_size: int
    partition_files: Optional[int] = None

    class Mode(enum.Enum):
        PUT = 1
        GET = -1

    @classmethod
    def from_path_mode_put(
            cls,
            max_size: int,
            path: Optional[str] = None
    ):
        return cls.from_path(mode=cls.Mode.PUT, max_size=max_size, path=path)

    @classmethod
    def from_path_mode_get(
            cls,
            max_size: int,
            path: Optional[str] = None
    ):
        return cls.from_path(mode=cls.Mode.GET, max_size=max_size, path=path)

    @classmethod
    def from_path(
            cls,
            mode: Mode,
            max_size: int,
            path: Optional[str] = None,
    ):
        path = path or SEMQ_DEFAULT_TRANSACTION_LOG_PATH
        youngest, oldest = "0000-00-00.000000.json", "9999-99-99.999999.json"
        files = 0
        for file in os.listdir(path):
            if file.startswith(("req", "del")):
                continue
            print("LOOP:", file)
            # Find the oldest file for "get" scenario
            if mode == cls.Mode.GET:
                oldest = oldest if oldest < file else file
            # Find the youngest file for "put" scenario
            elif mode == cls.Mode.PUT:
                youngest = youngest if youngest > file else file
            files += 1
        if not files:
            # TODO: Propagate empty scenario
            return None
        reference = oldest if mode == cls.Mode.GET else youngest if mode == cls.Mode.PUT else None
        print(reference, youngest, oldest)
        return cls(
            filepath=(
                get_new_partition_filepath(transaction_log_path=path)
                if not reference else
                os.path.join(path, reference)
            ),
            max_size=max_size,
            partition_files=files,
        ).create_if_not_exists()

    def get_request_file(self) -> RequestFile:
        return RequestFile(
            filepath=os.path.join(
                os.path.dirname(self.filepath),
                f"req-{os.path.basename(self.filepath)}"
            ),
            partition_file=self,
        ).create_if_not_exists()

    @classmethod
    def new(cls, max_size: int, partition_files: Optional[int] = None):
        return cls(
            filepath=get_new_partition_filepath(),
            max_size=max_size,
            partition_files=partition_files,
        ).create_if_not_exists()

    def append(self, item: str) -> 'PartitionFile':
        # Create the newline content
        line = json.dumps(
            {
                "created_at": dt.datetime.utcnow().isoformat(),
                "payload": item,
            }
        )
        with open(self.filepath, "r+") as file:
            for i, _ in enumerate(file):
                # Soft max validation; should we add a new line to current file or create a new one?
                if i >= self.max_size:
                    return PartitionFile.new(max_size=self.max_size).append(item=item)
            file.write(line + "\n")
        return self


class FileSystemQueue:

    def __init__(
            self,
            transaction_log_path: Optional[str] = None,
            partition_file_size: Optional[int] = None,
    ):
        self.transaction_log_path = transaction_log_path or SEMQ_DEFAULT_TRANSACTION_LOG_PATH
        self.partition_file_size = partition_file_size or SEMQ_DEFAULT_PARTITION_SIZE
        os.makedirs(self.transaction_log_path, exist_ok=True)

    @property
    def partition_file_operation_put(self) -> PartitionFile:
        return PartitionFile.from_path_mode_put(
            max_size=self.partition_file_size,
            path=self.transaction_log_path
        )

    @property
    def partition_file_operation_get(self) -> PartitionFile:
        return PartitionFile.from_path_mode_get(
            max_size=self.partition_file_size,
            path=self.transaction_log_path,
        )

    def put(self, item: str):
        self.partition_file_operation_put.append(item=item)

    def get_request(self) -> Tuple[RequestFile, str]:
        request_id = str(uuid.uuid4())
        request_file = self.partition_file_operation_get.get_request_file()
        return request_file.request(request_id=request_id), request_id

    def get(self) -> Optional[Dict]:
        request_file, request_id = self.get_request()
        with open(request_file.filepath, "r") as rfile:
            for i, line in enumerate(rfile):
                if line.startswith(request_id):
                    break
            else:
                # Request ID not found
                raise ValueError(
                    "Request ID %s not found in request file: %s",
                    request_id,
                    request_file.filepath
                )

        # Found request position; extracting same position from partition file
        with open(request_file.partition_file.filepath, "r") as pfile:
            for queue_position, line in enumerate(pfile):
                if queue_position == i:
                    return json.loads(line.strip())

