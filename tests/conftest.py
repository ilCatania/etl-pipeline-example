import os
from pathlib import Path
from typing import Callable, Union

import pytest


@pytest.fixture
def testfile() -> Callable[[Union[str, os.PathLike]], Path]:
    """Shortcut to obtain paths to files under `tests/__files`."""
    test_files_dir = Path(__file__).parent.joinpath("__files").resolve()
    assert test_files_dir.is_dir(), f"{test_files_dir} is not a directory!"

    def testfile_fn(p: Union[str, os.PathLike]) -> Path:
        return test_files_dir / p

    return testfile_fn
