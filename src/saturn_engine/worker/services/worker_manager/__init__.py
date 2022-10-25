import typing as t

import abc

from saturn_engine.core.api import LockResponse

from .. import Service
from .. import TOptions
from .. import TServices


class WorkerManagerClient(
    t.Generic[TServices, TOptions], Service[TServices, TOptions], abc.ABC
):
    name = "worker_manager_client"

    async def lock(self) -> LockResponse:
        raise NotImplementedError()
