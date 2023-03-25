import logging
import sys

import pandas as pd


def setup_logging() -> None:
    """Create a basic logging setup."""
    logging.basicConfig(
        level=logging.DEBUG,
        format="{asctime} {levelname:>5s} {module:>10}:{lineno:<3} {message}",
        style="{",
        handlers=[logging.StreamHandler(stream=sys.stdout)],
    )


def log_mem_usage(
    logger: logging.Logger,
    data: pd.DataFrame | pd.Series,
    data_name: str,
    level: int = logging.DEBUG,
) -> None:
    """Log the memory usage of a pandas dataframe or series in megabytes.

    :param logger: the logger to use.
    :param data: the pandas data to log memory usage for.
    :param data_name: the name of the pandas data.
    :param level: the logging level.
    """
    mu = data.memory_usage(deep=True)
    if isinstance(mu, pd.Series):
        mu = mu.sum()
    logger.log(level, "%s mem usage: %.02f MB", data_name, mu / 1e6)
