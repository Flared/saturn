from flask import Blueprint

bp = Blueprint("status", __name__, url_prefix="/api/status")


@bp.route("", methods=("GET",))
def get_status() -> str:
    return ""
