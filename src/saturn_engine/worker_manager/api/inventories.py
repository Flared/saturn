from flask import Blueprint

from saturn_engine.core.api import InventoriesResponse
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.worker_manager.config import config

bp = Blueprint("inventories", __name__, url_prefix="/api/inventories")


@bp.route("", methods=("GET",))
def get_inventories() -> Json[InventoriesResponse]:
    inventories = list(config().static_definitions.inventories.values())
    return jsonify(InventoriesResponse(items=inventories))
