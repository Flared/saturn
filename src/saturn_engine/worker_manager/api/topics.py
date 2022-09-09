from flask import Blueprint

from saturn_engine.core.api import TopicsResponse
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.worker_manager.app import current_app

bp = Blueprint("topics", __name__, url_prefix="/api/topics")


@bp.route("", methods=("GET",))  # type: ignore[type-var]
def get_job_definitions() -> Json[TopicsResponse]:
    topics = list(current_app.saturn.static_definitions.topics.values())
    return jsonify(TopicsResponse(items=topics))
