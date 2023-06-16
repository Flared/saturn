import dataclasses
from datetime import datetime

from saturn_engine.utils.asyncutils import DelayedThrottle

from .. import BaseServices
from .. import Service
from ..http_client import HttpClient


class Services(BaseServices):
    http_client: HttpClient


@dataclasses.dataclass
class Options:
    flush_delay: float = 1.0


class JobStateService(Service[Services, Options]):
    name = "job_state"

    Services = Services
    Options = Options

    async def open(self) -> None:
        self.delayed_flush = DelayedThrottle(self.flush, delay=self.options.flush_delay)

    def set_job_cursor(self, job_name: str, cursor: str) -> None:
        pass

    def set_job_completed(self, job_name: str, completed_at: datetime) -> None:
        pass

    def set_job_failed(
        self, job_name: str, *, error: Exception, failed_at: datetime
    ) -> None:
        pass

    def set_job_item_cursor(
        self, *, job_name: str, message_cursor: str, cursor: str
    ) -> None:
        pass

    async def flush(self) -> None:
        pass
