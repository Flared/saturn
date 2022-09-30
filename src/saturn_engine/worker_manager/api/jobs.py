from flask import Blueprint

from saturn_engine.core.api import JobInput
from saturn_engine.core.api import JobResponse
from saturn_engine.core.api import JobsResponse
from saturn_engine.core.api import JobsSyncResponse
from saturn_engine.core.api import ResetResponse
from saturn_engine.core.api import UpdateResponse
from saturn_engine.database import session_scope
from saturn_engine.models.job import Job
from saturn_engine.stores import jobs_store
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import abort
from saturn_engine.utils.flask import check_found
from saturn_engine.utils.flask import jsonify
from saturn_engine.utils.flask import marshall_request
from saturn_engine.worker_manager.app import current_app
from saturn_engine.worker_manager.services.sync import sync_jobs

bp = Blueprint("jobs", __name__, url_prefix="/api/jobs")


@bp.route("", methods=("GET",))
def get_jobs() -> Json[JobsResponse]:
    with session_scope() as session:
        return jsonify(
            JobsResponse(
                items=[
                    job.as_core_item() for job in jobs_store.get_jobs(session=session)
                ]
            )
        )


@bp.route("/<string:job_name>", methods=("GET",))
def get_job(job_name: str) -> Json[JobResponse]:
    with session_scope() as session:
        job = check_found(jobs_store.get_job(job_name, session=session))
        return jsonify(JobResponse(data=job.as_core_item()))


@bp.route("/<string:job_name>", methods=("PUT",))
def update_job(job_name: str) -> Json[UpdateResponse]:
    update_input = marshall_request(JobInput)
    if update_input.error and not update_input.completed_at:
        abort(
            http_code=400,
            error_code="CANNOT_ERROR_UNCOMPLETED_JOB",
        )
    with session_scope() as session:
        jobs_store.update_job(
            job_name,
            cursor=update_input.cursor,
            completed_at=update_input.completed_at,
            error=update_input.error,
            session=session,
        )
        return jsonify(UpdateResponse())


@bp.route("/reset", methods=("POST",))
def reset_job() -> Json[ResetResponse]:
    with session_scope() as session:
        jobs: list[Job] = jobs_store.get_jobs(session=session)
        for job in jobs:
            jobs_store.reset_job(job.name, session=session)

        return jsonify(ResetResponse())


@bp.route("/sync", methods=("POST",))
def post_sync() -> Json[JobsSyncResponse]:
    """Create jobs that are due to be scheduled."""
    with session_scope() as session:
        sync_jobs(
            static_definitions=current_app.saturn.static_definitions,
            session=session,
        )
    return jsonify(JobsSyncResponse())
