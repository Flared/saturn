from typing import Any
from typing import cast

from flask import Flask
from flask import current_app as _current_app

from saturn_engine.worker_manager.context import WorkerManagerContext


class SaturnApp(Flask):
    def __init__(
        self,
        worker_manager_context: WorkerManagerContext,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.saturn = worker_manager_context


current_app: SaturnApp = cast(SaturnApp, _current_app)
