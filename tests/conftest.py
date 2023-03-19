import os
from pathlib import Path
from typing import Callable, Union

import pytest

from etl_pipeline_example.create_dataset import (
    company_data,
    date_index,
    returns_data,
)


@pytest.fixture
def testfile() -> Callable[[Union[str, os.PathLike]], Path]:
    """Shortcut to obtain paths to files under `tests/__files`."""
    test_files_dir = Path(__file__).parent.joinpath("__files").resolve()
    assert test_files_dir.is_dir(), f"{test_files_dir} is not a directory!"

    def testfile_fn(p: Union[str, os.PathLike]) -> Path:
        return test_files_dir / p

    return testfile_fn


@pytest.fixture(scope="session")
def integration_test_data(testfile):
    """Create a lot of market and company return data for use during tests."""
    dates = date_index()
    comp = company_data(dates)
    market = returns_data(dates)
    company_returns_file = testfile("company_returns.pkl")
    market_returns_file = testfile("market_returns.pkl")
    comp.to_pickle(company_returns_file)
    market.to_pickle(market_returns_file)
    yield
    company_returns_file.unlink()
    market_returns_file.unlink()
