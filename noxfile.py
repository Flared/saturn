"""
Nox is used to run all testing and linting tools in differents environments.
To run locally, simply `pip install --user --upgrade nox` and then run `nox`
"""

import tempfile
from typing import Any
from typing import Sequence

import nox
from nox.sessions import Session

nox.options.sessions = "lint", "mypy", "tests", "tests_pipeline"
nox.options.reuse_existing_virtualenvs = True

python_all_versions = ["3.9"]
python_tool_version = "3.9"
pyfiles_locations = "example", "src", "tests", "noxfile.py", "mypy_stubs"


def install_project(
    session: Session,
    groups: Sequence[str] = (),
    worker_manager: bool = True,
) -> None:
    # Must wait for the release of Poetry 1.2.0 for
    # https://github.com/python-poetry/poetry/pull/4260/
    # poetry_options = []
    # if groups:
    #   poetry_options = "--with", ",".join(groups)
    # session.run("poetry", "install", *poetry_options, external=True)
    poetry_command: list[str] = ["poetry", "install"]
    if worker_manager:
        poetry_command.extend(["--extras", "worker-manager", "--extras", "ray"])
    session.run(*poetry_command, external=True)


def install_with_constraints(session: Session, *args: str, **kwargs: Any) -> None:
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python=python_all_versions)
def tests(session: Session) -> None:
    args = session.posargs
    install_project(session)
    install_with_constraints(session, "pytest", "pytest-asyncio", "pytest-icdiff")
    session.run("pytest", "-vv", *args)


@nox.session(python=python_all_versions)
def tests_pipeline(session: Session) -> None:
    args = session.posargs
    install_project(session)
    session.run("bash", "example/tests_pipeline", *args, external=True)


@nox.session(python=python_all_versions)
def tests_worker(session: Session) -> None:
    """Worker tests must pass without installing the worker-manager extra."""
    args = session.posargs
    install_project(session, worker_manager=False)
    install_with_constraints(session, "pytest", "pytest-asyncio", "pytest-icdiff")
    session.run("pytest", "tests/worker", *args)


@nox.session(python=python_tool_version)
def lint(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    install_project(session)
    install_with_constraints(
        session,
        "flake8",
        "flake8-bandit",
        "flake8-black",
        "flake8-breakpoint",
        "flake8-bugbear",
        "flake8-isort",
    )
    session.run("flake8", *args)


@nox.session(python=python_tool_version)
def mypy(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    install_project(session)
    install_with_constraints(session, "mypy")
    session.run("mypy", *args)


@nox.session(python=python_tool_version)
def format(session: Session) -> None:
    args = session.posargs or pyfiles_locations
    install_with_constraints(session, "black", "isort")
    session.run("black", *args)
    session.run("isort", *args)


@nox.session(python=python_tool_version)
def safety(session: Session) -> None:
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")
