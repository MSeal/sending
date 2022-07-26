# Releasing Sending

```
poetry version $VERSION
git commit -a -m "Releasing version $VERSION"
git tag -a "v$VERSION" -m "Release v$VERSION"
poetry build
poetry publish
```