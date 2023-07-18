import asyncio

from sqlalchemy.orm import sessionmaker

from saturn_engine.client.worker_manager import AbstractWorkerManagerClient
from saturn_engine.config import Config
from saturn_engine.core.api import FetchCursorsStatesInput
from saturn_engine.core.api import FetchCursorsStatesResponse
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import JobsStatesSyncResponse
from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.models.base import Base
from saturn_engine.stores import jobs_store
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.databases import Databases
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions
from saturn_engine.worker_manager.context import WorkerManagerContext
from saturn_engine.worker_manager.services.lock import lock_jobs
from saturn_engine.worker_manager.services.sync import sync_jobs


class StandaloneWorkerManagerClient(AbstractWorkerManagerClient):
    def __init__(
        self,
        *,
        config: Config,
        sessionmaker: sessionmaker,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.worker_id = config.c.worker_id
        self.context = WorkerManagerContext(config=config.c.worker_manager)
        self.max_assigned_items = config.c.worker_manager.work_items_per_worker

    async def init_db(self) -> None:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_init_db,
        )

    async def lock(self) -> LockResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_lock,
        )

    async def sync(self, sync_input: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_state,
            sync_input,
        )

    async def fetch_cursors_states(
        self, cursors: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_fetch_cursors_states,
            cursors,
        )

    async def sync_jobs(self) -> None:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self._sync_jobs,
        )

    # TODO: Eventually figure out some nice monadic pattern to support both
    # sync/async IO in stores.
    def _sync_init_db(self) -> None:
        Base.metadata.create_all(bind=self.sessionmaker.kw["bind"])

    def _sync_lock(self) -> LockResponse:
        with self.sessionmaker() as session:
            lock = lock_jobs(
                LockInput(worker_id=self.worker_id),
                max_assigned_items=self.max_assigned_items,
                static_definitions=self.context.static_definitions,
                session=session,
            )
            session.commit()
            return lock

    def _sync_state(self, sync_input: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        with self.sessionmaker() as session:
            jobs_store.sync_jobs_states(
                state=sync_input.state,
                session=session,
            )
            session.commit()
            return JobsStatesSyncResponse()

    def _sync_fetch_cursors_states(
        self, cursors_input: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        with self.sessionmaker() as session:
            cursors = jobs_store.fetch_cursors_states(
                cursors_input.cursors,
                session=session,
            )
            return FetchCursorsStatesResponse(cursors=cursors)

    def _sync_jobs(self) -> None:
        with self.sessionmaker() as session:
            sync_jobs(
                static_definitions=self.context.static_definitions,
                session=session,
            )
            session.commit()
