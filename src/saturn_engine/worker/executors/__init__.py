from typing import Type

from abc import ABC
from abc import abstractmethod

from saturn_engine.core import PipelineResults
from saturn_engine.utils.inspect import import_name
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import Services


class Executor(ABC):
    def __init__(self, services: Services) -> None:
        pass

    @abstractmethod
    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        ...

    @property
    @abstractmethod
    def concurrency(self) -> int:
        pass

    async def close(self) -> None:
        pass


def get_executor_class(path: str) -> Type[Executor]:
    klass = BUILTINS.get(path)
    if klass:
        return klass

    return import_name(path)


from .process import ProcessExecutor

BUILTINS: dict[str, Type[Executor]] = {
    "ProcessExecutor": ProcessExecutor,
}

try:
    from .ray import RayExecutor

    BUILTINS["RayExecutor"] = RayExecutor
except ImportError:
    pass

try:
    from .arq.executor import ARQExecutor

    BUILTINS["ARQExecutor"] = ARQExecutor
except ImportError:
    pass
