import flask
from flask import Blueprint

from saturn_engine.core.api import FetchCursorsStatesInput
from saturn_engine.core.api import FetchCursorsStatesResponse
from saturn_engine.core.api import JobInput
from saturn_engine.core.api import JobResponse
from saturn_engine.core.api import JobsResponse
from saturn_engine.core.api import JobsStartResponse
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import JobsStatesSyncResponse
from saturn_engine.core.api import JobsSyncResponse
from saturn_engine.core.api import StartJobInput
from saturn_engine.core.api import UpdateResponse
from saturn_engine.database import session_scope
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


@bp.route("/sync", methods=("POST",))
def post_sync() -> Json[JobsSyncResponse]:
    """Create jobs that are due to be scheduled."""
    with session_scope() as session:
        # We reset static definition at each jobs sync
        current_app.saturn.load_static_definition(session=session)
        sync_jobs(
            static_definitions=current_app.saturn.static_definitions,
            session=session,
        )
    return jsonify(JobsSyncResponse())


@bp.route("/_states", methods=("PUT", "POST"))
def post_sync_jobs_states() -> Json[JobsStatesSyncResponse]:
    """Sync jobs states to the database in batch."""
    sync_input = marshall_request(JobsStatesSyncInput)
    with session_scope() as session:
        jobs_store.sync_jobs_states(
            state=sync_input.state,
            session=session,
        )
    return jsonify(JobsStatesSyncResponse())


@bp.route("/_states/fetch", methods=("POST",))
def post_fetch_states() -> Json[FetchCursorsStatesResponse]:
    """Fetch cursors states."""
    cursors_input = marshall_request(FetchCursorsStatesInput)
    with session_scope() as session:
        cursors = jobs_store.fetch_cursors_states(
            cursors_input.cursors,
            session=session,
        )
    return jsonify(FetchCursorsStatesResponse(cursors=cursors))


@bp.route("/_start", methods=("POST",))
def post_start_job() -> Json[JobsStartResponse]:
    restart: bool = flask.request.args.get("restart", "false") == "true"
    start_input = marshall_request(StartJobInput)
    if not start_input.name and not start_input.job_definition_name:
        abort(http_code=400, error_code="MUST_SPECIFY_JOB")
    elif start_input.name and start_input.job_definition_name:
        abort(http_code=400, error_code="MUST_SPECIFY_ONE_JOB")

    with session_scope() as session:
        try:
            job = jobs_store.start(
                start_input=start_input,
                static_definitions=current_app.saturn.static_definitions,
                session=session,
                restart=restart,
            )
        except ValueError as e:
            abort(http_code=400, error_code="JOB_START_ERROR", message=str(e))

        return jsonify(JobsStartResponse(name=job.name))
