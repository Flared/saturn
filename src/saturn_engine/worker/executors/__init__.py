from typing import Type

from abc import ABC
from abc import abstractmethod

from saturn_engine.core import PipelineResults
from saturn_engine.core import api
from saturn_engine.utils.inspect import import_name
from saturn_engine.utils.options import OptionsSchema
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services import Services


class Executor(ABC, OptionsSchema):
    name: str

    @abstractmethod
    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        pass

    @property
    @abstractmethod
    def concurrency(self) -> int:
        pass

    async def close(self) -> None:
        pass


def build_executor(
    executor_definition: api.ComponentDefinition, *, services: Services
) -> Executor:
    klass = BUILTINS.get(executor_definition.type)
    if klass is None:
        klass = import_name(executor_definition.type)
    if klass is None:
        raise ValueError(f"Unknown topic type: {executor_definition.type}")
    if not issubclass(klass, Executor):
        raise ValueError(f"{klass} must be an Executor")
    options = {"name": executor_definition.name} | executor_definition.options
    executor = klass.from_options(options, services=services)
    executor.name = executor_definition.name
    return executor


from .process import ProcessExecutor

BUILTINS: dict[str, Type[Executor]] = {
    "ProcessExecutor": ProcessExecutor,
}

try:
    from .arq.executor import ARQExecutor

    BUILTINS["ARQExecutor"] = ARQExecutor
except ImportError:
    pass
