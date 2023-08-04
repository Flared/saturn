from typing import Optional
from typing import Type

import logging
import re
from dataclasses import dataclass
from functools import lru_cache
from textwrap import dedent
from types import TracebackType

from saturn_engine.core.api import QueueItem
from saturn_engine.core.error import ErrorMessageArgs
from saturn_engine.core.pipeline import PipelineOutput
from saturn_engine.core.pipeline import PipelineResults
from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils.traceback_data import TracebackData
from saturn_engine.worker.executors.bootstrap import RemoteException


@dataclass
class ExceptionDetails:
    message: str
    exception_type: str
    module: str
    lineno: int
    traceback: TracebackData


@dataclass
class ExceptionFilter:
    message_pattern: Optional[re.Pattern]
    exception_type: Optional[re.Pattern]
    module_pattern: Optional[re.Pattern]
    lineno: int


def process_pipeline_exception(
    *,
    queue: QueueItem,
    message: TopicMessage,
    exc_type: Type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType,
) -> Optional[PipelineResults]:
    outputs = queue.output
    exc_details = get_exception_details(exc_type, exc_value, exc_traceback)

    # Match the exception and consume it if found, else reraise the exception
    for channel in outputs.keys():
        if not is_output_error_handler(channel):
            continue

        try:
            exc_filter = parse_warning_filter(channel, escape=False)

            if not does_error_match(exception=exc_details, exception_filter=exc_filter):
                continue

            return PipelineResults(
                outputs=[
                    PipelineOutput(
                        channel=channel,
                        message=TopicMessage(
                            args={
                                "cause": message,
                                "error": ErrorMessageArgs(
                                    type=exc_details.exception_type,
                                    module=exc_details.module,
                                    message=exc_details.message,
                                    traceback=exc_details.traceback,
                                ),
                            }
                        ),
                    )
                ],
                resources=[],
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "Failed to process error channel", extra={"data": {"channel": channel}}
            )
    return None


def does_error_match(
    *, exception: ExceptionDetails, exception_filter: ExceptionFilter
) -> bool:
    exc_type = exception.exception_type
    exc_type_re = exception_filter.exception_type
    type_matched = (
        exc_type_re is None or exc_type_re.match(exception.exception_type) is not None
    )
    if not type_matched and exc_type_re and "." in exc_type:
        exc_type = exc_type.rsplit(".", 1)[-1]
        type_matched = exc_type_re.match(exc_type) is not None

    return (
        (
            exception_filter.message_pattern is None
            or exception_filter.message_pattern.match(exception.message) is not None
        )
        and type_matched
        and (
            exception_filter.module_pattern is None
            or exception_filter.module_pattern.match(exception.module) is not None
        )
        and (
            exception_filter.lineno == 0 or exception.lineno == exception_filter.lineno
        )
    )


def get_exception_details(
    exc_type: Type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType,
) -> ExceptionDetails:
    exc_type_name = f"{exc_type.__module__}.{exc_type.__name__}"
    if isinstance(exc_value, RemoteException):
        import_path = ".".join(
            [
                exc_value.remote_traceback.exc_module,
                exc_value.remote_traceback.exc_type,
            ]
        )
        exc_type_name = import_path
        traceback = exc_value.remote_traceback
    else:
        traceback = TracebackData.from_exc_info(exc_type, exc_value, exc_traceback)

    return ExceptionDetails(
        message=str(exc_value),
        exception_type=exc_type_name,
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
    if category and escape:
        category = re.escape(category) + r"\Z"
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
        message_pattern=re.compile(message) if message else None,
        exception_type=re.compile(category) if category else None,
        module_pattern=re.compile(module) if module else None,
        lineno=lineno,
    )
