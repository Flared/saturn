from flask import Blueprint

from saturn_engine.core.api import JobDefinitionsResponse
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.worker_manager.config import config

bp = Blueprint("job_definitions", __name__, url_prefix="/api/job_definitions")


@bp.route("", methods=("GET",))
def get_job_definitions() -> Json[JobDefinitionsResponse]:
    job_definitions = list(config().static_definitions.job_definitions.values())
    return jsonify(JobDefinitionsResponse(items=job_definitions))
