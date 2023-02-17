import os
import json
import datetime as dt
from typing import Optional
from dataclasses import dataclass

from .utils import get_new_partition_filepath
from .settings import (
    SEMQ_DEFAULT_TRANSACTION_LOG_PATH,
    SEMQ_DEFAULT_PARTITION_SIZE
)


@dataclass
class PartitionFile:
    filepath: str
    max_size: int

    @classmethod
    def from_transaction_log_path(cls, max_size: int, path: Optional[str] = None):
        path = path or SEMQ_DEFAULT_TRANSACTION_LOG_PATH
        current = None
        for file in os.listdir(path):
            current = current if (current or "0") > file else file
        return cls(
            filepath=(
                get_new_partition_filepath(transaction_log_path=path)
                if not current else
                os.path.join(path, current)
            ),
            max_size=max_size,
        )


    @classmethod
    def new(cls, max_size: int):
        return cls(
            filepath=get_new_partition_filepath(),
            max_size=max_size,
        )

    @property
    def size(self):
        if not os.path.exists(self.filepath):
            return 0
        with open(self.filepath, "r") as file:
            for i, _ in enumerate(file):
                continue
            return i + 1

    def append(self, item: str) -> 'PartitionFile':
        if self.size >= self.max_size:
            return PartitionFile.new(max_size=self.max_size).append(item=item)
        line = json.dumps(
            {
                "created_at": dt.datetime.utcnow().isoformat(),
                "payload": item
            }
        )
        with open(self.filepath, "a") as file:
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
        self.partition_file = PartitionFile.from_transaction_log_path(
            max_size=self.partition_file_size,
            path=self.transaction_log_path
        )

    def put(self, item: str):
        self.partition_file = self.partition_file.append(item=item)
