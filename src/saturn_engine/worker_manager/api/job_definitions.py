from flask import Blueprint

from saturn_engine.core.api import JobDefinitionsResponse
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.worker_manager.app import current_app

bp = Blueprint("job_definitions", __name__, url_prefix="/api/job_definitions")


@bp.route("", methods=("GET",))  # type: ignore[type-var]
def get_job_definitions() -> Json[JobDefinitionsResponse]:
    job_definitions = list(
        current_app.saturn.static_definitions.job_definitions.values()
    )
    return jsonify(JobDefinitionsResponse(items=job_definitions))
