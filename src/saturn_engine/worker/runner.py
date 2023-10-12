import typing as t

import asyncio
import logging
import signal

from saturn_engine.config import Config
from saturn_engine.config import default_config_with_env

from .broker import Broker


def set_term_handler(handler: t.Callable[[], t.Any]) -> None:
    loop = asyncio.get_running_loop()
    for signame in ["SIGINT", "SIGTERM"]:
        loop.add_signal_handler(getattr(signal, signame), handler)


def unset_term_handler() -> None:
    loop = asyncio.get_running_loop()
    for signame in ["SIGINT", "SIGTERM"]:
        loop.remove_signal_handler(getattr(signal, signame))


def standalone_config(config: Config) -> Config:
    base_services = config.c.services_manager.base_services.copy()
    services = config.c.services_manager.services.copy()

    try:
        base_services.remove("saturn_engine.worker.services.api_client.ApiClient")
        services.remove("saturn_engine.worker.services.databases.Databases")
    except ValueError:
        pass
    base_services = [
        "saturn_engine.worker.services.databases.Databases",
        "saturn_engine.worker.services.api_client.StandaloneApiClient",
    ] + base_services

    engines = config.r.get("databases", {}).get("sync_engines", {}).copy()
    engines.setdefault("default", "sqlite:///standalone.db")
    config = config.load_object(
        {
            "services_manager": {
                "base_services": base_services,
                "services": services,
            },
            "databases": {"sync_engines": engines, "engines": {}},
        }
    )

    return config


async def async_main() -> None:
    config = default_config_with_env()
    if config.c.standalone:
        config = standalone_config(config)

    broker = Broker(config)

    def stop() -> None:
        broker.stop()
        unset_term_handler()

    set_term_handler(stop)
    await broker.run()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger = logging.getLogger(__name__)

    loop = asyncio.get_event_loop()
    asyncio.run(async_main())
    if tasks := asyncio.all_tasks(loop):
        logger.error("Leftover tasks: %s", tasks)


if __name__ == "__main__":
    main()
