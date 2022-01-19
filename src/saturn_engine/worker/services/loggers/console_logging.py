import logging
import logging.config

from saturn_engine.utils.serializer import human_encode

from .. import MinimalService


class ConsoleLogging(MinimalService):
    name = "console_logging"

    async def open(self) -> None:
        # if not setup_structlog():
        setup_logging()


# def setup_structlog() -> bool:
#     try:
#         import structlog
#     except ImportError:
#         logging.warning(
#             "Console logging setup without structlog. For a more pleasant and "
#             "colorful experience, install saturn with console (pip install "
#             "saturn-engine[console])"
#         )
#         return False


class ExtraFormatter(logging.Formatter):
    def_keys = [
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "asctime",
    ]

    def format(self, record: logging.LogRecord) -> str:
        string = super().format(record)
        extra = {k: v for k, v in record.__dict__.items() if k not in self.def_keys}
        extra.update(extra.pop("data", {}))
        if extra:
            string += " - " + human_encode(extra, compact=True)
        return string


def setup_logging() -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "standard": {
                    "class": __name__ + "." + ExtraFormatter.__name__,
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "level": "DEBUG",
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",  # Default is stderr
                },
            },
            "root": {"handlers": ["default"], "level": "DEBUG"},
        }
    )
