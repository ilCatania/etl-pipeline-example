import logging
from enum import Enum
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pandas.tseries.offsets import BaseOffset

from etl_pipeline_example.log_utils import log_mem_usage

log = logging.getLogger(__name__)


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
    company_data: pd.Series | pd.DataFrame, market_data: pd.Series, window: int
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


def run_pipeline(data_dir: Path):
    """Execute the data pipeline."""
    # Load the company_returns data
    log.info("Starting pipeline...")
    company_data = pd.read_pickle(data_dir / "company_returns.pkl")
    log_mem_usage(log, company_data, "Original company data")
    # downcast to reduce memory footprint
    company_data = pd.to_numeric(company_data, downcast="float")
    log_mem_usage(log, company_data, "Downcasted company data")
    # resample so that it is of business day frequency
    log.info("Resampling company data to business day...")
    company_data_resampled = resample_company_returns(
        company_data, "B", ResampleStrategy.INTERPOLATE_LINEAR
    )
    log_mem_usage(log, company_data_resampled, "BDay resampled company data")
    # Store this data in an efficient way. Describe the method and the file size
    # once stored.
    #   I'm storing the data in an arrow dataset made up of parquet partitions,
    #   for now arbitrarily setting a number of partitions
    log.info("Storing company data...")
    n_partitions = 64
    company_ids = company_data_resampled.index.get_level_values(0)
    partition_values = company_ids % n_partitions
    company_data_with_partitions = pd.DataFrame(company_data_resampled).assign(
        partition=partition_values
    )
    log_mem_usage(
        log, company_data_with_partitions, "Company data with partition info"
    )
    store_dir = data_dir / "store"
    store_dir.mkdir(exist_ok=True)
    company_table = pa.Table.from_pandas(company_data_with_partitions)
    ds.write_dataset(
        company_table,
        store_dir / "company_data",
        format="parquet",
        partitioning=["partition"],
        existing_data_behavior="delete_matching",
    )

    # Load the market_returns
    log.info("Loading market data...")
    market_data = pd.read_pickle(data_dir / "market_returns.pkl")
    # make sure the series index has a name, or we'll get weird errors later on
    if market_data.index.name is None:
        market_data.index = market_data.index.rename("date")
    log_mem_usage(log, market_data, "Original market data")
    market_data = pd.to_numeric(market_data, downcast="float")
    log_mem_usage(log, market_data, "Downcasted market data")
    # calculate for each company the correlation to the market on a rolling
    # 2 year basis. State any modelling assumptions made.
    #   Assumption: company returns for missing days are interpolated. Change
    #   the assumption by using a different ResampleStrategy above
    log.info("Calculating correlations...")
    two_years = 262 * 2  # 2 years window in business days
    corrs = []
    for n_part in range(n_partitions):
        partition_path = store_dir / f"company_data/{n_part}"
        if not partition_path.exists():
            continue
        log.debug("Calculating correlation part %i of %i", n_part, n_partitions)
        company_part = pd.read_parquet(partition_path, engine="pyarrow")
        corr = rolling_corr(company_part, market_data, window=two_years)
        corrs.append(corr)
    corr_result = pd.concat(corrs)
    log.info("Saving correlations...")
    corr_result.to_csv(store_dir / "result_corr.csv")
