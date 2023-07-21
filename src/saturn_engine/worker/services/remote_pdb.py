import typing as t

import dataclasses
import signal
import threading

from saturn_engine.utils.log import getLogger
from saturn_engine.worker.services import BaseServices
from saturn_engine.worker.services import Service


@dataclasses.dataclass
class Options:
    host: str = "localhost"
    port: int = 31337
    signal: int = signal.SIGUSR2


class RemotePdb(Service[BaseServices, Options]):
    name = "remote_pdb"

    Options = Options

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.logger = getLogger(__name__, self)

    async def open(self) -> None:
        try:
            pass

            if threading.current_thread() != threading.main_thread():
                raise ValueError("Must be on main thread")
            signal.signal(self.options.signal, self.remote_debug_signal)
        except Exception:
            self.logger.exception("Failed to init remote pdb")

    def remote_debug_signal(self, *args: t.Any) -> None:
        from remote_pdb import RemotePdb

        RemotePdb(self.options.host, self.options.port).set_trace()
