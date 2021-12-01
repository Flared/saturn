import threading
import time
from datetime import datetime

from croniter import croniter

from saturn_engine.database import async_session_scope
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow
from saturn_engine.worker_manager.config import config
from saturn_engine.worker_manager.config.declarative import StaticDefinitions

_SYNC_LOCK = threading.Lock()


async def sync_jobs() -> None:
    if not _SYNC_LOCK.locked():
        with _SYNC_LOCK:
            static_definitions: StaticDefinitions = config().static_definitions

            async with async_session_scope() as session:
                for job_definition in static_definitions.job_definitions.values():
                    last_job = await jobs_store.get_last_job(
                        session=session,
                        job_definition_name=job_definition.name,
                    )

                    if (
                        not last_job
                        or croniter(
                            job_definition.minimal_interval,
                            last_job.started_at,
                        ).get_next(ret_type=datetime)
                        < utcnow()
                    ):
                        job_name: str = f"{job_definition.name}-{int(time.time())}"
                        queue = queues_store.create_queue(
                            session=session, name=job_name
                        )
                        jobs_store.create_job(
                            name=job_name,
                            session=session,
                            queue_name=queue.name,
                            job_definition_name=job_definition.name,
                        )
