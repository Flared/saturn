import threading
import time
from datetime import datetime

from croniter import croniter
from flask import Blueprint

from saturn_engine.core.api import JobInput
from saturn_engine.core.api import JobResponse
from saturn_engine.core.api import JobsResponse
from saturn_engine.core.api import JobsSyncResponse
from saturn_engine.core.api import UpdateResponse
from saturn_engine.database import async_session_scope
from saturn_engine.stores import jobs_store
from saturn_engine.utils import utcnow
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import check_found
from saturn_engine.utils.flask import jsonify
from saturn_engine.utils.flask import marshall_request
from saturn_engine.worker_manager.config import config
from saturn_engine.worker_manager.config.declarative import StaticDefinitions

bp = Blueprint("jobs", __name__, url_prefix="/api/jobs")


@bp.route("", methods=("GET",))
async def get_jobs() -> Json[JobsResponse]:
    async with async_session_scope() as session:
        return jsonify(
            JobsResponse(
                items=[
                    job.as_core_item()
                    for job in await jobs_store.get_jobs(session=session)
                ]
            )
        )


@bp.route("/<string:job_name>", methods=("GET",))
async def get_job(job_name: str) -> Json[JobResponse]:
    async with async_session_scope() as session:
        job = check_found(await jobs_store.get_job(job_name, session=session))
        return jsonify(JobResponse(data=job.as_core_item()))


@bp.route("/<string:job_name>", methods=("PUT",))
async def update_job(job_name: str) -> Json[UpdateResponse]:
    update_input = marshall_request(JobInput)
    async with async_session_scope() as session:
        await jobs_store.update_job(
            job_name,
            cursor=update_input.cursor,
            completed_at=update_input.completed_at,
            session=session,
        )
        return jsonify(UpdateResponse())


_SYNC_LOCK = threading.Lock()


@bp.route("/sync", methods=("POST",))
async def post_sync() -> Json[JobsSyncResponse]:
    """Create jobs that are due to be scheduled."""
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
                    jobs_store.create_job(
                        name=f"{job_definition.name}-{int(time.time())}",
                        session=session,
                        queue_name=job_definition.template.input.name,
                        job_definition_name=job_definition.name,
                    )

        return jsonify(JobsSyncResponse())
