from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def testfiles() -> Path:
    """Shortcut to obtain paths to files under `tests/__files`."""
    test_files_dir = Path(__file__).parent.joinpath("__files").resolve()
    assert test_files_dir.is_dir(), f"{test_files_dir} is not a directory!"
    return test_files_dir
