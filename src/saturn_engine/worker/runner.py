import asyncio
import logging
import signal

from .broker import Broker

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    broker = Broker()
    loop = asyncio.get_event_loop()

    for signame in ["SIGINT", "SIGTERM"]:
        loop.add_signal_handler(getattr(signal, signame), broker.stop)

    loop.run_until_complete(broker.run())
    if tasks := asyncio.all_tasks(loop):
        logger.error("Leftover tasks: %s", tasks)
