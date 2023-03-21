from itertools import chain

import numpy as np
import pandas as pd
from pandas._testing import assert_series_equal

from etl_pipeline_example.create_dataset import date_index
from etl_pipeline_example.pipeline import rolling_corr


def test_rolling_corr_trivial():
    company_idx_names = ["companyid", "date"]
    dates = date_index("2010-01-01", "2010-01-31")
    n_dates = len(dates)
    company_ids = [0, 1]
    company_idx = pd.MultiIndex.from_product(
        [company_ids, dates], names=company_idx_names
    )
    # build returns for two companies, both perfectly correlated but in
    # opposite directions
    company_values = list(chain(range(n_dates), range(n_dates, 0, -1)))
    company_returns = pd.Series(
        name="returns", data=company_values, index=company_idx
    )
    market_returns = pd.Series(
        name="returns", index=dates, data=list(range(n_dates))
    )
    actual = rolling_corr(company_returns, market_returns, 5)
    expected_data = [np.NaN] * 4 + [1] * 17 + [np.NaN] * 4 + [-1] * 17
    expected = pd.Series(name="returns", data=expected_data, index=company_idx)
    assert_series_equal(actual, expected)
