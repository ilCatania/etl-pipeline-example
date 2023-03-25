import pandas as pd
from pandas._testing import assert_index_equal, assert_series_equal

from etl_pipeline_example.create_dataset import (
    company_data,
    date_index,
    returns_data,
    write_dataset,
)


def test_date_index_default_args():
    actual = date_index()
    expected = pd.bdate_range("2000-01-01", pd.Timestamp.today(), name="date")
    assert_index_equal(actual, expected)


def test_create_default_dates():
    """Check that default dates are used if none specified."""
    expected_dates = date_index()
    cd = company_data(n_companies=1, n_dates=len(expected_dates))
    cd_dates = cd.index.get_level_values(1)
    assert set(cd_dates) == set(expected_dates)  # order is randomized
    ret = returns_data()
    assert_index_equal(ret.index, expected_dates)


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


def test_write_dataset(tmp_path):
    expected_comp = pd.Series(
        name="returns",
        index=pd.MultiIndex.from_tuples(
            [
                (0, pd.Timestamp("2022-01-20")),
                (0, pd.Timestamp("2022-01-19")),
                (1, pd.Timestamp("2022-01-20")),
                (1, pd.Timestamp("2022-01-10")),
                (2, pd.Timestamp("2022-01-13")),
                (2, pd.Timestamp("2022-01-18")),
            ],
            names=["companyid", "date"],
        ),
        data=[0.03285, 0.0134, 0.01889, -0.001, 0.005720839515684686, -0.0073],
    )
    expected_mkt = pd.Series(
        name="returns",
        index=pd.bdate_range("2022-01-10", "2022-01-20", name="date"),
        data=[
            0.00378,
            -0.0054,
            0.00193,
            -0.0136,
            0.0060,
            -0.0122,
            0.00004,
            -0.0009,
            -0.0064,
        ],
    )
    write_dataset(
        tmp_path,
        history_start="2022-01-10",
        history_end="2022-01-20",
        n_companies=3,
        n_dates=2,
        random_seed=12,
    )
    actual_comp = pd.read_pickle(tmp_path / "company_returns.pkl")
    actual_mkt = pd.read_pickle(tmp_path / "market_returns.pkl")
    assert_series_equal(actual_comp, expected_comp, atol=1e-4)
    assert_series_equal(actual_mkt, expected_mkt, atol=1e-4)
