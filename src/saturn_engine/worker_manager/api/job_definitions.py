from flask import Blueprint
from flask import Response
from flask import jsonify

from saturn_engine.worker_manager.config import config

bp = Blueprint("job_definitions", __name__, url_prefix="/api/job_definitions")


@bp.route("", methods=("GET",))
async def get_job_definitions() -> Response:
    job_definitions = list(config().static_definitions.job_definitions.values())
    return jsonify({"job_definitions": job_definitions})
