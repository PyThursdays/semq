import datetime as dt
from typing import Optional

from .settings import (
    get_logger,
    SEMQ_DEFAULT_HELLO_WORLD,
)

logger = get_logger(name=__name__)


class CLI:

    def __init__(self):
        self.execution_timestamp = dt.datetime.utcnow()

    def hello(self, name: Optional[str] = None) -> str:
        return f"Hello, {name or SEMQ_DEFAULT_HELLO_WORLD}!"

