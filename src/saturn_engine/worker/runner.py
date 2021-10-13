import asyncio
import logging

from .broker import Broker

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    broker = Broker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(broker.run())
