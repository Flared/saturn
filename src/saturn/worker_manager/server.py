from flask import Flask


def get_app() -> Flask:
    app = Flask(__name__)

    from .api.status import bp as bp_status

    app.register_blueprint(bp_status)

    return app


def main() -> None:
    app = get_app()
    app.run()


if __name__ == "__main__":
    main()
