import os
import uuid
import json
import enum
import time
import shutil
import datetime as dt
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

from .utils import get_new_partition_filepath
from .exceptions import (
    UnavailablePartitionFiles,
)
from .settings import (
    get_logger,
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

    def soft_delete(self, trash_dirpath: Optional[str] = None, only_rename: bool = False) -> bool:
        rename = FilePrefix.apply_prefix_delete(filepath=self.filepath)
        try:
            if only_rename:
                return os.rename(
                    self.filepath,
                    rename
                ) or True
            if trash_dirpath:
                return shutil.move(
                    self.filepath,
                    os.path.join(trash_dirpath, os.path.basename(rename)),
                ) or True
            raise ValueError("Soft Delete Misconfiguration")
        except FileNotFoundError:
            logger.warning("Soft delete failed due to file-not-found error")


@dataclass
class RequestFile(AbstractFile):
    filepath: str
    partition_file: 'PartitionFile'
    trash_dirpath: Optional[str] = None

    def refresh(self, wait_seconds: int = -1) -> 'RequestFile':
        partition_file_configs = {
            "max_size": self.partition_file.max_size,
            "path": os.path.dirname(self.partition_file.filepath),
            "wait_seconds": wait_seconds,
        }
        # Delete partition file
        self.partition_file.soft_delete(trash_dirpath=self.trash_dirpath)
        # Delete request file
        self.soft_delete(trash_dirpath=self.trash_dirpath)
        # Create new partition file
        partition_file = self.partition_file.from_path_mode_get(**partition_file_configs)
        return partition_file.get_request_file(trash_dirpath=self.trash_dirpath)

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
            if file.startswith(prefix_options) or not file.endswith(".json"):
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
        filepath = get_new_partition_filepath(file_path=path) if not files else os.path.join(path, reference)
        return cls(
            filepath=filepath,
            max_size=max_size,
            partition_files=files,
            item_hashing=item_hashing,
        ).create_if_not_exists()

    def get_request_file(self, trash_dirpath: Optional[str] = None) -> RequestFile:
        return RequestFile(
            filepath=FilePrefix.apply_prefix_request(filepath=self.filepath),
            partition_file=self,
            trash_dirpath=trash_dirpath
        ).create_if_not_exists()

    @classmethod
    def new(cls, path: str, max_size: int, partition_files: Optional[int] = None):
        return cls(
            filepath=get_new_partition_filepath(file_path=path),
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
                    pfile = PartitionFile.new(path=os.path.dirname(self.filepath), max_size=self.max_size)
                    return pfile.append(item=item)
            file.write(line)
        return payload, self
