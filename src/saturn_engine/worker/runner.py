import asyncio
import logging
import signal

from saturn_engine.config import Config
from saturn_engine.config import default_config

from .broker import Broker


async def async_main() -> None:
    loop = asyncio.get_running_loop()
    config = Config().load_object(default_config).load_envvar("SATURN_SETTINGS")
    broker = Broker(config)
    for signame in ["SIGINT", "SIGTERM"]:
        loop.add_signal_handler(getattr(signal, signame), broker.stop)
    await broker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger = logging.getLogger(__name__)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main())
    if tasks := asyncio.all_tasks(loop):
        logger.error("Leftover tasks: %s", tasks)
