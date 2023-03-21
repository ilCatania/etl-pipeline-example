import numpy as np
import pandas as pd
from pandas._testing import assert_series_equal

from etl_pipeline_example.pipeline import (
    ResampleStrategy,
    resample_company_returns,
)


def test_resample_company_single():
    """Check that resampling returns for a single company works."""
    company_idx_names = ["companyid", "date"]
    company_dates = [
        pd.Timestamp("2023-02-13"),  # Monday
        pd.Timestamp("2023-02-20"),  # Monday
        pd.Timestamp("2023-02-23"),  # Thursday
    ]
    company_idx = pd.MultiIndex.from_product(
        [[0], company_dates], names=company_idx_names
    )
    company_resampled_idx = pd.MultiIndex.from_product(
        [[0], pd.date_range("2023-02-13", "2023-02-23", freq="B")],
        names=company_idx_names,
    )
    company_data = pd.Series(
        name="returns", data=[0.05, -0.03, 0.02], index=company_idx
    )
    expected = pd.Series(
        name="returns",
        data=[
            0.05,
            np.NaN,
            np.NaN,
            np.NaN,
            np.NaN,
            -0.03,
            np.NaN,
            np.NaN,
            0.02,
        ],
        index=company_resampled_idx,
    )
    actual = resample_company_returns(
        company_data, target_freq="B", strategy=ResampleStrategy.NA_FILL
    )
    assert_series_equal(actual, expected)

    expected2 = expected.fillna(0)
    actual2 = resample_company_returns(
        company_data, target_freq="B", strategy=ResampleStrategy.ZERO_FILL
    )
    assert_series_equal(actual2, expected2)

    expected3 = pd.Series(
        name="returns",
        data=[
            0.05,
            0.034,
            0.018,
            0.002,
            -0.014,
            -0.03,
            -0.013333,
            0.003333,
            0.02,
        ],
        index=company_resampled_idx,
    )
    actual3 = resample_company_returns(
        company_data,
        target_freq="B",
        strategy=ResampleStrategy.INTERPOLATE_LINEAR,
    )
    assert_series_equal(actual3, expected3, atol=1e-3)
