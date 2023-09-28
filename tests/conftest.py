import pytest


def pytest_addoption(parser):
    parser.addoption("--skipcdf", action="store_true", default=False, help="Skip tests that use CDF API.")


def pytest_configure(config):
    config.addinivalue_line("markers", "cdf: mark test as needing CDF to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--skipcdf"):
        # --skipcdf not given in cli: do not skip cdf tests
        return
    skip_cdf = pytest.mark.skip(reason="skipped using --skipcdf option")
    for item in items:
        if "cdf" in item.keywords:
            item.add_marker(skip_cdf)
