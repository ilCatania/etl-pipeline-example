import numpy as np
import pandas as pd
from pandas._testing import assert_series_equal

from etl_pipeline_example.pipeline import (
    ResampleStrategy,
    resample_company_returns,
)


def test_resample_company_single():
    """Check that resampling returns for a single company works."""
    company_data = pd.Series(
        name="returns",
        data=[0.05, -0.03, 0.02],
        index=pd.MultiIndex.from_product(
            [
                [0],
                [
                    pd.Timestamp("2023-02-13"),  # Monday
                    pd.Timestamp("2023-02-20"),  # Monday
                    pd.Timestamp("2023-02-23"),  # Thursday
                ],
            ],
            names=["companyid", "date"],
        ),
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
        index=pd.MultiIndex.from_product(
            [[0], pd.date_range("2023-02-13", "2023-02-23", freq="B")],
            names=["companyid", "date"],
        ),
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
