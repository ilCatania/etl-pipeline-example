import os
from pathlib import Path
from typing import Callable, Union

import pytest


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
    from itertools import repeat

    import numpy as np
    import pandas as pd

    # make randomisation deterministic
    pd.core.common.random_state(42)

    dates = pd.bdate_range("2000-01-01", pd.Timestamp.today())
    n_companies = 5000
    n_dates = 4000
    index = [
        list(
            zip(repeat(i), np.random.choice(dates, size=n_dates, replace=False))
        )
        for i in range(n_companies)
    ]
    index = [x for y in index for x in y]
    index = pd.MultiIndex.from_tuples(index, names=["companyid", "date"])
    returns = np.random.normal(loc=0, scale=0.012, size=n_companies * n_dates)
    df = pd.Series(index=index, data=returns, name="returns")

    mkt_returns = np.random.normal(loc=0, scale=0.008, size=len(dates))
    market = pd.Series(index=dates, data=mkt_returns)

    testfiles_dir = Path(__file__).parent.joinpath("__files")
    company_returns_file = testfiles_dir / "company_returns.pkl"
    market_returns_file = testfiles_dir / "market_returns.pkl"
    df.to_pickle(company_returns_file)
    market.to_pickle(market_returns_file)
    yield
    company_returns_file.unlink()
    market_returns_file.unlink()
