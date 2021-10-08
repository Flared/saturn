import dataclasses

import desert
import flask_apispec
import marshmallow
from flask import Blueprint

bp = Blueprint("lock", __name__, url_prefix="/api/lock")


@dataclasses.dataclass
class LockInput:
    worker_id: str

    @classmethod
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)


@dataclasses.dataclass
class LockResponse:
    items: list[str]  # TODO: this will be list[WorkItem]

    @classmethod
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)


@bp.route("", methods=("POST",))
@flask_apispec.use_kwargs(LockInput.schema())
@flask_apispec.marshal_with(LockResponse.schema())
async def post_lock(lock_input: LockInput) -> LockResponse:
    return LockResponse(items=[])
