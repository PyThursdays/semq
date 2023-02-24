import os

from semq.settings import SEMQ_DEFAULT_METASTORE_PATH
from semq.q import SimpleExternalQueue


def cleanup_wrapper(everything: bool = False, **kwargs) -> str:
    queue = validate_queue_attributes(**kwargs)
    queue.cleanup(everything=everything)
    return "ok"


def validate_queue_attributes(**kwargs) -> SimpleExternalQueue:

    queue_name = kwargs.get("name")
    if not queue_name:
        raise ValueError("Queue name needs to be provided")

    metastore_path = kwargs.get("metastore_path", SEMQ_DEFAULT_METASTORE_PATH)
    if not os.path.exists(metastore_path):
        raise ValueError("Metastore path does not exists: %s", metastore_path)

    return SimpleExternalQueue(
        name=queue_name,
        metastore_path=metastore_path,
        item_hashing=True
    )
        
