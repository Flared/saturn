import logging
from collections.abc import Iterable
from collections.abc import Iterator

from saturn_engine.core import PipelineMessage
from saturn_engine.core import PipelineOutput
from saturn_engine.core import TopicMessage


def bootstrap_pipeline(message: PipelineMessage) -> list[PipelineOutput]:
    logger = logging.getLogger("pipeline")
    logger.info("Executing %s", message)
    result = message.execute()

    # Ensure result is an iterator.
    results: Iterator
    if isinstance(result, Iterable):
        results = iter(result)
    elif not isinstance(result, Iterator):
        if isinstance(result, (TopicMessage, PipelineOutput)):
            results = iter([result])
        else:
            logger.error("Invalid result type: %s", result.__class__)
            results = iter([])
    else:
        results = result

    # Convert result into a list of PipelineOutput.
    outputs: list[PipelineOutput] = []
    for output in results:
        if isinstance(output, PipelineOutput):
            outputs.append(output)
        elif isinstance(output, TopicMessage):
            outputs.append(PipelineOutput(channel="default", message=output))
        else:
            logger.error("Invalid output type: %s", output.__class__)

    return outputs
