import contextlib
import logging
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResult
from saturn_engine.core import ResourceUsed
from saturn_engine.core import TopicMessage
from saturn_engine.utils.traceback_data import TracebackData
from saturn_engine.worker.pipeline_message import PipelineMessage


def bootstrap_pipeline(message: PipelineMessage) -> PipelineResult:
    logger = logging.getLogger("pipeline")
    logger.info("Executing %s", message)
    execute_result = message.execute()

    # Ensure result is an iterator.
    results: Iterator
    if execute_result is None:
        results = iter([])
    elif isinstance(execute_result, Iterable):
        results = iter(execute_result)
    elif not isinstance(execute_result, Iterator):
        if isinstance(execute_result, (TopicMessage, PipelineOutput, ResourceUsed)):
            results = iter([execute_result])
        else:
            logger.error("Invalid result type: %s", execute_result.__class__)
            results = iter([])
    else:
        results = execute_result

    # Convert result into a list of PipelineOutput.
    outputs: list[PipelineOutput] = []
    resources: list[ResourceUsed] = []
    for result in results:
        if isinstance(result, PipelineOutput):
            outputs.append(result)
        elif isinstance(result, TopicMessage):
            outputs.append(PipelineOutput(channel="default", message=result))
        elif isinstance(result, ResourceUsed):
            resources.append(result)
        else:
            logger.error("Invalid result type: %s", result.__class__)

    return PipelineResult(outputs=outputs, resources=resources)


class RemoteException(Exception):
    def __init__(self, tb: TracebackData):
        super().__init__(tb)
        self.remote_traceback = tb

    @classmethod
    def from_exception(cls, exception: Exception) -> "RemoteException":
        tb = TracebackData.from_exception(exception)
        return cls(tb)

    def __str__(self) -> str:
        return (
            self.remote_traceback.format_exception_only()
            + "\nRemoteException "
            + "".join(self.remote_traceback.format())
        )

    def __repr__(self) -> str:
        stype = f"RemoteException[{self.remote_traceback.exc_type}]"
        if self.remote_traceback.exc_str.startswith("("):
            return stype + self.remote_traceback.exc_str
        return stype + f"({self.remote_traceback.exc_str})"


@contextlib.contextmanager
def wrap_remote_exception() -> Generator[None, None, None]:
    try:
        yield
    except Exception as e:
        raise RemoteException.from_exception(e) from None
