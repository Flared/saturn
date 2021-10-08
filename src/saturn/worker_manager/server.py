from typing import Optional

from flask import Flask

from saturn.database import async_scoped_session
from saturn.database import create_all
from saturn.worker_manager.http_errors import register_http_exception_error_handler


def get_app() -> Flask:
    app = Flask(__name__)

    from .api.jobs import bp as bp_jobs
    from .api.lock import bp as bp_lock
    from .api.status import bp as bp_status

    app.register_blueprint(bp_status)
    app.register_blueprint(bp_jobs)
    app.register_blueprint(bp_lock)

    @app.teardown_appcontext  # type: ignore
    async def shutdown_session(response_or_exc: Optional[BaseException]) -> None:
        await async_scoped_session().remove()

    register_http_exception_error_handler(app)

    return app


def main() -> None:
    app = get_app()
    create_all()
    app.run()


if __name__ == "__main__":
    main()
