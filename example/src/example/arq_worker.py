import arq

from saturn_engine.worker.executors.arq.worker import WorkerSettings
from saturn_engine.worker.services.loggers.console_logging import setup


def main() -> None:
    setup()
    arq.run_worker(WorkerSettings)  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
