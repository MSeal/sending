import nox


def bootstrap_poetry(session):
    session.install("poetry")
    session.run("poetry", "install", "-E", "redis")


def run(session, *args, **kwargs):
    session.run("poetry", "run", *args, **kwargs)


@nox.session
def lint(session):
    bootstrap_poetry(session)
    run(session, "black", "sending")
    run(session, "black", "tests")

    run(session, "isort", "sending")
    run(session, "isort", "tests")


@nox.session
def lint_check(session):
    bootstrap_poetry(session)
    run(session, "black", "--diff", "--check", "sending")
    run(session, "black", "--diff", "--check", "tests")

    run(session, "isort", "--diff", "--check", "sending")
    run(session, "isort", "--diff", "--check", "tests")

    run(session, "flake8", "sending", "--count", "--show-source", "--statistics", "--benchmark")
    run(session, "flake8", "tests", "--count", "--show-source", "--statistics", "--benchmark")


@nox.session(python = ["3.8", "3.9"])
def test(session):
    bootstrap_poetry(session)
    run(session, "pytest", env={"SENDING__ENABLE_LOGGING": "True"})


@nox.session(python = ["3.8", "3.9"])
def test_redis(session):
    # Requires you install and run redis-server first
    bootstrap_poetry(session)
    run(
        session,
        "pytest",
        "-m",
        "redis",
        env={"SENDING__ENABLE_LOGGING": "True", "REDIS_DSN": "redis://localhost:6379"},
    )
