# Releasing Sending

## Prerequisites

1. Verify poetry 1.2 is installed and properly configured.
2. Create an API token scoped to Sending on pypi. Make sure that two factor auth is enabled for pypi.
3. Setup poetry to use your pypi token: `poetry config pypi-token.pypi <your token>`

## Create a Pull Request for Release

Branch protection for `main` is enabled for this repo. You will do the following steps.

1. Update the `CHANGELOG.md`
2. Update `sending/__init__.py` version
3. Update `pyproject.toml` version

Create a pull request from your release branch to `main` by:

```
poetry version $VERSION
git commit -a -m "Releasing version $VERSION"
git tag -a "v$VERSION" -m "Release v$VERSION"
git push origin <release branch name>
```

## Publish to pypi

After the release PR is approved:

```
poetry build
poetry publish
```

Verify on pypi that package is there and can be installed.
