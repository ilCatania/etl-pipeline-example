import pandas as pd
from pandas._testing import assert_index_equal, assert_series_equal

from etl_pipeline_example.create_dataset import (
    company_data,
    date_index,
    returns_data,
)


def test_date_index_default_args():
    actual = date_index()
    expected = pd.bdate_range("2000-01-01", pd.Timestamp.today(), name="date")
    assert_index_equal(actual, expected)


def test_create_companies(testfile):
    """Check that the company returns data function always generates the same
    data when given the same seed.
    """
    expected = pd.read_csv(
        testfile("expected_companies.csv"),
        parse_dates=["date"],
        dtype={"companyid": "int64", "returns": "float64"},
        index_col=["companyid", "date"],
    ).squeeze("columns")
    dates = date_index("2000-01-01", "2023-03-20")
    actual = company_data(dates, n_companies=3, n_dates=5, random_seed=5)
    assert_series_equal(actual, expected)


def test_create_returns(testfile):
    """Check that the market returns data function always generates the same
    data when given the same seed.
    """
    expected = pd.read_csv(
        testfile("expected_returns.csv"),
        parse_dates=True,
        dtype="float64",
        index_col="date",
    ).squeeze("columns")
    dates = date_index("2023-02-12", "2023-02-26")
    actual = returns_data(dates, random_seed=13)
    assert_series_equal(actual, expected, check_freq=False)
