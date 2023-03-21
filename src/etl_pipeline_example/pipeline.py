from enum import Enum

import pandas as pd
from pandas.tseries.offsets import BaseOffset


class ResampleStrategy(Enum):
    """Strategy to adopt when resampling data."""

    ZERO_FILL = "zero-fill"
    """Fill with zeroes when returns are missing."""
    NA_FILL = "na-fill"
    """Fill with NAs when returns are missing."""
    INTERPOLATE_LINEAR = "interpolate-linear"
    """Fill with linearly interpolated values."""


def resample_company_returns(
    company_data: pd.Series,
    target_freq: str | BaseOffset = "B",
    strategy: ResampleStrategy = ResampleStrategy.ZERO_FILL,
) -> pd.Series:
    """
    Resample an input company data series.

    The resulting series covers all the dates for the input frequency in the
    input period. This assumes the input company data series has a multi index
    containing companyid and date.

    :param company_data:the company data
    :param target_freq: the target frequency to resample to.
    :param strategy: the resample strategy, e.g. fill with zeroes where returns
        are missing.
    :return: the resampled returns.
    """
    groups = company_data.groupby(level="companyid")
    resampled = groups.resample(rule=target_freq, level="date")
    if strategy == ResampleStrategy.ZERO_FILL:
        return resampled.sum()
    elif strategy == ResampleStrategy.NA_FILL:
        return resampled.sum(min_count=1)
    elif strategy == ResampleStrategy.INTERPOLATE_LINEAR:
        return resampled.sum(min_count=1).interpolate("linear")
    else:
        raise NotImplementedError(f"Not implemented: {strategy}")


def rolling_corr(
    company_data: pd.Series, market_data: pd.Series, window: int
) -> pd.Series:
    """
    Calculate rolling correlation between company and market data.

    :param company_data: the company data
    :param market_data: the market data
    :param window: the window to use
    :return: the correlation
    """
    res = company_data.groupby("companyid").rolling(window).corr(market_data)

    # FIXME figure out why there's a duplicate companyid index column and remove
    #  this hack
    res.index = res.index.droplevel()

    return res
