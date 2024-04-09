"""
Nox is used to run all testing and linting tools in differents environments.
To run locally, simply `pip install --user --upgrade nox` and then run `nox`
"""

import nox
from nox_poetry import Session
from nox_poetry import session as nox_session

nox.options.sessions = "lint", "mypy", "tests", "tests_worker", "example_tests"
nox.options.reuse_existing_virtualenvs = True

python_all_versions = ["3.10", "3.11"]
python_tool_version = "3.11"
pyfiles_locations = "example", "src", "tests", "noxfile.py", "mypy_stubs"
docs_locations = ("docs",)
# These are package that are imported in the tests or this noxfile. Not all
# package required to run lint and tests.
tests_packages = [
    "pytest",
    "pytest-asyncio",
    "pytest-icdiff",
    "pytest-mock",
    "freezegun",
    "opentelemetry-sdk",
    "sentry-sdk",
]
mypy_packages = [
    "pytest",
    "pytest-mock",
    "types-freezegun",
    "mypy-typing-asserts",
]


def all_extras(session: Session) -> str:
    return ".[" + ",".join(session.poetry.poetry.config.extras) + "]"


@nox_session(python=python_all_versions)
@nox.parametrize("sqlalchemy", ["1.4.52", "2.0.29"])
def tests(session: Session, sqlalchemy: str) -> None:
    args = session.posargs
    session.install(".[worker-manager,structlog]", *tests_packages)
    session._session.install(f"sqlalchemy=={sqlalchemy}")
    session.run(
        "pytest",
        "-vv",
        *args,
        env={"PY_IGNORE_IMPORTMISMATCH": "1"},
    )


@nox_session(python=python_all_versions)
def example_tests(session: Session) -> None:
    args = session.posargs
    session.install(".")
    session.run("bash", "example/run_tests", *args, external=True)


@nox_session(python=python_all_versions)
def tests_worker(session: Session) -> None:
    """Worker tests must pass without installing the worker-manager extra."""
    args = session.posargs
    session.install(".", *tests_packages)
    session.run("pytest", "tests/worker", *args)


@nox_session(python=python_tool_version)
def lint(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    session.install(
        ".",
        "flake8",
        "flake8-bandit",
        "flake8-black",
        "flake8-breakpoint",
        "flake8-bugbear",
        "flake8-isort",
    )
    session.run("flake8", *args)


@nox_session(python=python_tool_version)
def mypy(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    session.install(
        all_extras(session),
        "mypy",
        # Packages required to check tests typing.
        *mypy_packages,
    )
    session.run("mypy", "--show-error-codes", *args)


@nox_session(python=python_tool_version)
def format(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    session.install(
        ".",
        "black",
        "isort",
        "autoflake8",
        "rstfmt",
    )
    session.run("black", *args)
    session.run("isort", *args)
    session.run(
        "autoflake8",
        "--in-place",
        "--recursive",
        "--remove-unused-variables",
        "--exit-zero-even-if-changed",
        *args,
    )


@nox_session(python=python_tool_version)
def safety(session: Session) -> None:
    session.install("safety")
    requirements = session.poetry.export_requirements()
    session.run("safety", "check", f"--file={requirements}", "--full-report")


@nox_session(python=python_tool_version)
def docs(session: Session) -> None:
    """Build sphinx docs."""
    session.posargs
    session.install(".[arq,statsd,sentry,tracer]")
    session.cd("docs")
    session._session.install("-r", "requirements.txt")
    session.run("make", "html", external=True)


@nox_session(python=python_tool_version)
def watch_docs(session: Session) -> None:
    """Build sphinx docs with autoreload."""
    session.posargs
    session.install(".[arq,statsd,sentry,tracer]")
    session.cd("docs")
    session._session.install("-r", "requirements.txt")
    session.run("make", "livehtml", external=True)
