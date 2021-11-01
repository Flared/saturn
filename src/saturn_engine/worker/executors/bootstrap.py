import logging

from saturn_engine.core import PipelineMessage


def bootstrap_pipeline(message: PipelineMessage) -> None:
    logger = logging.getLogger("pipeline")
    logger.info("Executing %s", message)
    message.execute()
