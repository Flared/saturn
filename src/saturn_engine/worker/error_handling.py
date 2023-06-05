from typing import Optional
from typing import Type
from typing import cast

import re
from dataclasses import dataclass
from functools import lru_cache
from textwrap import dedent
from types import TracebackType

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.error import ErrorMessageArgs
from saturn_engine.core.pipeline import PipelineOutput
from saturn_engine.core.pipeline import PipelineResults
from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.traceback_data import TracebackData
from saturn_engine.worker.executors.bootstrap import RemoteException


@dataclass
class ExceptionDetails:
    message: str
    exception_type: Type[BaseException]
    module: str
    lineno: int
    traceback: TracebackData


@dataclass
class ExceptionFilter:
    message_pattern: Optional[re.Pattern]
    exception_type: Type[BaseException]
    module_pattern: Optional[re.Pattern]
    lineno: int


def process_pipeline_exception(
    outputs: dict[str, list[ComponentDefinition]],
    exc_value: BaseException,
    exc_traceback: TracebackType,
) -> Optional[PipelineResults]:
    # Match the exception and consume it if found, else reraise the exception
    for channel in outputs.keys():
        if not is_output_error_handler(channel):
            continue

        exc_filter = parse_warning_filter(channel, escape=False)
        exc_details = get_exception_details(exc_value, exc_traceback)

        if not does_error_match(exception=exc_details, exception_filter=exc_filter):
            continue

        return PipelineResults(
            outputs=[
                PipelineOutput(
                    channel=channel,
                    message=TopicMessage(
                        args={
                            "error": ErrorMessageArgs(
                                type=exc_details.exception_type.__name__,
                                module=exc_details.module,
                                message=str(exc_value),
                                traceback=exc_details.traceback,
                            )
                        }
                    ),
                )
            ],
            resources=[],
        )
    return None


def does_error_match(
    *, exception: ExceptionDetails, exception_filter: ExceptionFilter
) -> bool:
    return (
        (
            exception_filter.message_pattern is None
            or exception_filter.message_pattern.match(exception.message) is not None
        )
        and issubclass(exception.exception_type, exception_filter.exception_type)
        and (
            exception_filter.module_pattern is None
            or exception_filter.module_pattern.match(exception.module) is not None
        )
        and (
            exception_filter.lineno == 0 or exception.lineno == exception_filter.lineno
        )
    )


def resolve_exception(exception: str) -> Type[BaseException]:
    if "." not in exception:
        return extra_inspect.import_name(f"builtins.{exception}")
    return extra_inspect.import_name(exception)


def get_exception_details(
    exc_value: BaseException, exc_traceback: TracebackType
) -> ExceptionDetails:
    exc_type = exc_value.__class__
    if issubclass(exc_type, RemoteException):
        exc: RemoteException = cast(RemoteException, exc_value)
        import_path = ".".join(
            [
                exc.remote_traceback.exc_module,
                exc.remote_traceback.exc_type,
            ]
        )
        exc_type = resolve_exception(import_path)
        traceback = exc.remote_traceback
    else:
        traceback = TracebackData.from_exc_info(exc_type, exc_value, exc_traceback)

    return ExceptionDetails(
        message=str(exc_value),
        exception_type=exc_type,
        module=traceback.stack[-1].module,
        lineno=traceback.stack[-1].lineno,
        traceback=traceback,
    )


def is_output_error_handler(output: str) -> bool:
    return output.startswith("error:")


@lru_cache(maxsize=50)
def parse_warning_filter(arg: str, *, escape: bool) -> ExceptionFilter:
    """Parse a warnings filter string.
    This is copied from warnings._setoption with the following changes:
    * Does not apply the filter.
    * Escaping is optional.
    """
    __tracebackhide__ = True
    error_template = dedent(
        f"""\
        while parsing the following warning configuration:
          {arg}
        This error occurred:
        {{error}}
        """
    )

    parts = arg.split(":")
    if len(parts) > 5:
        doc_url = (
            "https://docs.python.org/3/library/warnings.html#describing-warning-filters"
        )
        error = dedent(
            f"""\
            Too many fields ({len(parts)}), expected at most 5 separated by colons:
              action:message:category:module:line
            For more information please consult: {doc_url}
            """
        )
        raise Exception(error_template.format(error=error))

    while len(parts) < 5:
        parts.append("")

    _, message, category, module, lineno_ = (s.strip() for s in parts)

    if message and escape:
        message = re.escape(message)
    if module and escape:
        module = re.escape(module) + r"\Z"
    if lineno_:
        try:
            lineno = int(lineno_)
            if lineno < 0:
                raise ValueError("number is negative")
        except ValueError as e:
            raise Exception(
                error_template.format(error=f"invalid lineno {lineno_!r}: {e}")
            ) from e
    else:
        lineno = 0
    return ExceptionFilter(
        message_pattern=None if message == "" else re.compile(message),
        exception_type=resolve_exception(category),
        module_pattern=None if module == "" else re.compile(module),
        lineno=lineno,
    )
