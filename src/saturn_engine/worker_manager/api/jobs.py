from flask import Blueprint

from saturn_engine.core.api import JobInput
from saturn_engine.core.api import JobResponse
from saturn_engine.core.api import JobsResponse
from saturn_engine.core.api import UpdateResponse
from saturn_engine.database import async_session_scope
from saturn_engine.stores import jobs_store
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import check_found
from saturn_engine.utils.flask import jsonify
from saturn_engine.utils.flask import marshall_request

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


@bp.route("/<int:job_id>", methods=("GET",))
async def get_job(job_id: int) -> Json[JobResponse]:
    async with async_session_scope() as session:
        job = check_found(await jobs_store.get_job(job_id, session=session))
        return jsonify(JobResponse(data=job.as_core_item()))


@bp.route("/<int:job_id>", methods=("PUT",))
async def update_job(job_id: int) -> Json[UpdateResponse]:
    update_input = marshall_request(JobInput)
    async with async_session_scope() as session:
        await jobs_store.update_job(
            job_id,
            cursor=update_input.cursor,
            completed_at=update_input.completed_at,
            session=session,
        )
        return jsonify(UpdateResponse())
