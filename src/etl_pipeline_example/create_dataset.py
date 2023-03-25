from itertools import repeat
from pathlib import Path
from typing import Any, Tuple

import numpy as np
import pandas as pd
from pandas.tseries.offsets import BaseOffset

DEFAULT_RANDOM_SEED = 42


def date_index(
    history_start: pd.Timestamp | str = pd.Timestamp("2000-01-01"),
    history_end: pd.Timestamp | str | None = None,
    freq: str | BaseOffset = "B",
    name: str = "date",
) -> pd.DatetimeIndex:
    """
    Shortcut method to create date ranges with reasonable defaults.

    :param history_start: the start date for the generated index
    :param history_end: the end date for the generated index, defaults to today
    :param freq: the frequency, defaults to business day
    :param name: the index name
    :return: a new ``DatetimeIndex``
    """
    history_end = history_end or pd.Timestamp.today()
    return pd.date_range(history_start, history_end, name=name, freq=freq)


def company_data(
    dates: pd.DatetimeIndex | None = None,
    n_companies: int = 500,
    n_dates: int = 4000,
    random_seed: Any = DEFAULT_RANDOM_SEED,
) -> pd.Series:
    """Create a series of company returns.

    :param dates: the datetimes to sample from
    :param n_companies: the number of companies in the resulting series.
    :param n_dates: the number of datetime samples to take.
    :param random_seed: an optional random seed for repeatable results.
    :return: a series of randomized company returns.
    """
    if dates is None:
        dates = date_index()
    # make randomisation deterministic
    np.random.seed(random_seed)
    index_n = [
        list(
            zip(repeat(i), np.random.choice(dates, size=n_dates, replace=False))
        )
        for i in range(n_companies)
    ]
    index_all = [x for y in index_n for x in y]
    index = pd.MultiIndex.from_tuples(index_all, names=["companyid", "date"])
    returns = np.random.normal(loc=0, scale=0.012, size=n_companies * n_dates)
    return pd.Series(index=index, data=returns, name="returns")


def returns_data(
    dates: pd.DatetimeIndex | None = None,
    random_seed: Any = DEFAULT_RANDOM_SEED,
) -> pd.Series:
    """Create a series of market returns by date.

    :param dates: the datetimes to use when generating returns.
    :param random_seed: an optional random seed for repeatable results.
    :return: a series of randomized market returns.
    """
    if dates is None:
        dates = date_index()
    # make randomisation deterministic
    np.random.seed(random_seed)
    mkt_returns = np.random.normal(loc=0, scale=0.008, size=len(dates))
    return pd.Series(index=dates, data=mkt_returns, name="returns")


def write_dataset(
    dataset_dir: Path,
    history_start: str | pd.Timestamp = "2000-01-01",
    history_end: str | pd.Timestamp | None = None,
    n_companies: int = 5000,
    n_dates: int = 4000,
    random_seed: Any = DEFAULT_RANDOM_SEED,
) -> Tuple[Path, Path]:
    """Write a dataset of company and market returns using pickle.

    :param history_start: the history start date.
    :param history_end: the history end date, or None to use today.
    :param dataset_dir: the directory to write into.
    :param n_companies: the number of companies in the dataset.
    :param n_dates: the number of dates for which companies have returns.
    :param random_seed: an optional random seed for repeatable results.
    :return: paths to the company and market return files respectively.
    """
    dates = date_index(history_start=history_start, history_end=history_end)
    comp = company_data(
        dates, n_companies=n_companies, n_dates=n_dates, random_seed=random_seed
    )
    market = returns_data(dates, random_seed=random_seed)
    cr = dataset_dir / "company_returns.pkl"
    mr = dataset_dir / "market_returns.pkl"
    dataset_dir.mkdir(exist_ok=True, parents=True)
    comp.to_pickle(cr)
    market.to_pickle(mr)
    return cr, mr
