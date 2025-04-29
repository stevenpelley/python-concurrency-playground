import pytest

from pyconcurrencyplayground import utils


@pytest.fixture(autouse=True)
def configure_logging() -> None:
    utils.setup_logging()
