from typing import Optional

from saturn_engine.config import default_config_with_env
from saturn_engine.database import create_all
from saturn_engine.database import scoped_session
from saturn_engine.database import session_scope
from saturn_engine.utils.flask import register_http_exception_error_handler
from saturn_engine.worker_manager.app import SaturnApp
from saturn_engine.worker_manager.app import current_app
from saturn_engine.worker_manager.context import WorkerManagerContext
from saturn_engine.worker_manager.services.sync import sync_jobs


def get_app(
    config: Optional[dict] = None,
) -> SaturnApp:
    worker_manager_context = WorkerManagerContext(
        config=default_config_with_env().c.worker_manager,
    )

    app = SaturnApp(
        worker_manager_context,
        __name__,
    )

    if config:
        app.config.from_mapping(config)

    from .api.inventories import bp as bp_inventories
    from .api.job_definitions import bp as bp_job_definitions
    from .api.jobs import bp as bp_jobs
    from .api.lock import bp as bp_lock
    from .api.status import bp as bp_status
    from .api.topics import bp as bp_topics
    from .api.topologies import bp as bp_topologies

    app.register_blueprint(bp_status)
    app.register_blueprint(bp_jobs)
    app.register_blueprint(bp_job_definitions)
    app.register_blueprint(bp_topics)
    app.register_blueprint(bp_lock)
    app.register_blueprint(bp_inventories)
    app.register_blueprint(bp_topologies)

    @app.teardown_appcontext  # type: ignore
    def shutdown_session(response_or_exc: Optional[BaseException]) -> None:
        scoped_session().remove()

    register_http_exception_error_handler(app)

    return app


def init_all(app: Optional[SaturnApp] = None) -> None:
    if app is None:
        app = get_app()

    with app.app_context():
        create_all()
        with session_scope() as session:
            current_app.saturn.load_static_definition(session=session)
            sync_jobs(
                static_definitions=current_app.saturn.static_definitions,
                session=session,
            )


def main() -> None:
    app = get_app()
    init_all(app=app)
    app.run(
        host=app.saturn.config.flask_host,
        port=app.saturn.config.flask_port,
    )


if __name__ == "__main__":
    main()
