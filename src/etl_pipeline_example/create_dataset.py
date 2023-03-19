from itertools import repeat
from typing import Any

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
