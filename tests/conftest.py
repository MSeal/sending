import pytest


def pytest_collection_modifyitems(config, items):
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return  # let pytest handle this

    for item in items:
        if "redis" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="redis tests not enabled"))
        if "jupyter" in item.keywords:
            item.add_marker(pytest.mark.skip(reason="jupyter tests not enabled"))
