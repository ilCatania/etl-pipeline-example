import logging
from typing import Any

import pandas as pd

from etl_pipeline_example.log_utils import log_mem_usage, setup_logging


def test_setup_logging(monkeypatch):
    called = False
    args_match = False
    expected_args = {
        "level": logging.DEBUG,
        "format": Any,
        "style": "{",
        "handlers": Any,
    }

    def patch_logging_config(**kwargs):
        nonlocal called, args_match
        called = True
        for arg, expected in expected_args.items():
            actual = kwargs.get(arg, None)
            if actual is None:
                raise AssertionError(f"Missing argument: {arg}!")
            if expected is Any:
                continue
            if expected != actual:
                raise AssertionError(
                    f"Unexpected argument value for {arg}, "
                    f"expected: {expected}, actual: {actual}!"
                )
        args_match = True

    monkeypatch.setattr("logging.basicConfig", patch_logging_config)
    setup_logging()
    assert called, "Logging basic config was not called!"
    assert args_match, "Logging basic config was called with unexpected args!"


def test_log_mem_usage_series(monkeypatch, caplog):
    test_logger = logging.getLogger("test")
    s = pd.Series(dtype="int64")

    def patch_mem_usage(index=True, deep=False):
        return 2.2 * 1e6 if index and deep else 0

    monkeypatch.setattr(s, "memory_usage", patch_mem_usage)
    expected_log_tuple = ("test", 30, "test series mem usage: 2.20 MB")
    with caplog.at_level(logging.WARNING):
        log_mem_usage(test_logger, s, "test series", level=logging.WARNING)
    assert caplog.record_tuples == [expected_log_tuple]


def test_log_mem_usage_df(monkeypatch, caplog):
    test_logger = logging.getLogger("test")
    df = pd.DataFrame()

    def patch_mem_usage(index=True, deep=False):
        if index and deep:
            return pd.Series(
                data=[3 * 1e6, 2 * 1e5],
                index=["Index", "float64"],
                dtype="int64",
            )
        else:
            return pd.Series()

    monkeypatch.setattr(df, "memory_usage", patch_mem_usage)
    expected_log_tuple = ("test", 30, "test df mem usage: 3.20 MB")
    with caplog.at_level(logging.WARNING):
        log_mem_usage(test_logger, df, "test df", level=logging.WARNING)
    assert caplog.record_tuples == [expected_log_tuple]
