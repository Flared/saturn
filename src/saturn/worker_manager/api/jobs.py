from flask import Blueprint
from flask import Response
from flask import jsonify

from saturn.database import async_session_scope
from saturn.stores import jobs_store

bp = Blueprint("jobs", __name__, url_prefix="/api/jobs")


@bp.route("", methods=("GET",))
async def get_jobs() -> Response:
    async with async_session_scope() as session:
        return jsonify(
            {"jobs": [job.asdict() for job in await jobs_store.get_jobs(session)]}
        )
