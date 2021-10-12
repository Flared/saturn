from flask import Blueprint
from flask import Response
from flask import jsonify
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from saturn.database import async_session_scope
from saturn.models import Job

bp = Blueprint("jobs", __name__, url_prefix="/api/jobs")


@bp.route("", methods=("GET",))
async def get_jobs() -> Response:
    async with async_session_scope() as session:
        jobs: list[Job] = (
            (
                await session.execute(
                    select(Job).options(
                        joinedload(Job.queue),
                    )
                )
            )
            .scalars()
            .all()
        )
        return jsonify({"jobs": [job.asdict() for job in jobs]})
