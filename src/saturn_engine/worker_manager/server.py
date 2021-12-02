from typing import Optional

from flask import Flask

from saturn_engine.database import create_all
from saturn_engine.database import scoped_session
from saturn_engine.utils.flask import register_http_exception_error_handler
from saturn_engine.worker_manager.config import config
from saturn_engine.worker_manager.services.sync import sync_jobs


def get_app() -> Flask:
    app = Flask(__name__)

    from .api.inventories import bp as bp_inventories
    from .api.job_definitions import bp as bp_job_definitions
    from .api.jobs import bp as bp_jobs
    from .api.lock import bp as bp_lock
    from .api.status import bp as bp_status

    app.register_blueprint(bp_status)
    app.register_blueprint(bp_jobs)
    app.register_blueprint(bp_job_definitions)
    app.register_blueprint(bp_lock)
    app.register_blueprint(bp_inventories)

    @app.teardown_appcontext  # type: ignore
    def shutdown_session(response_or_exc: Optional[BaseException]) -> None:
        scoped_session().remove()

    register_http_exception_error_handler(app)

    return app


def init_all() -> None:
    sync_jobs()


def main() -> None:
    app = get_app()
    create_all()
    init_all()
    app.run(
        host=config().flask_host,
        port=config().flask_port,
    )


if __name__ == "__main__":
    main()
