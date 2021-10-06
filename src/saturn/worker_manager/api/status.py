from flask import Blueprint

bp = Blueprint("status", __name__, url_prefix="/api/status")


@bp.route("", methods=("GET",))
async def get_status() -> str:
    return ""
