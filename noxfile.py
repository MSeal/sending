import nox
import nox_poetry

LINT_PATHS = ["sending", "noxfile.py", "tests"]

nox.options.reuse_existing_virtualenv = True
nox.options.sessions = ["lint", "test"]


@nox_poetry.session(python=["3.8", "3.9", "3.10"])
def test(session: nox_poetry.Session):
    session.run_always(
        "poetry", "install", "-E", "redis", "-E", "jupyter", "-E", "websockets", external=True
    )
    session.run(
        "pytest",
        "-v",
        "--cov=sending",
        env={"SENDING__ENABLE_LOGGING": "True", "REDIS_DSN": "redis://localhost:6379"},
    )


@nox_poetry.session(python="3.8")
def lint(session: nox_poetry.Session):
    session.notify("black_check")
    session.notify("flake8")
    session.notify("isort_check")


@nox_poetry.session(python="3.8")
def flake8(session: nox_poetry.Session):
    session.install("flake8")
    session.run("flake8", *LINT_PATHS, "--count", "--show-source", "--statistics", "--benchmark")


@nox_poetry.session(python="3.8")
def black_check(session: nox_poetry.Session):
    session.install("black")
    session.run("black", "--check", *LINT_PATHS)


@nox_poetry.session(python="3.8")
def isort_check(session: nox_poetry.Session):
    session.install("isort")
    session.run("isort", "--diff", "--check", *LINT_PATHS)


@nox_poetry.session(python="3.8")
def blacken(session: nox_poetry.Session):
    session.install("black")
    session.run("black", *LINT_PATHS)


@nox_poetry.session(python="3.8")
def isort_apply(session: nox_poetry.Session):
    session.install("isort")
    session.run("isort", *LINT_PATHS)


@nox_poetry.session(python="3.8")
def generate_coverage_xml(session: nox_poetry.Session):
    session.install("coverage[toml]")
    session.run("coverage", "xml")
