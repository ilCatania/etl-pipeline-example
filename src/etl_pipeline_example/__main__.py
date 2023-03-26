import logging
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from etl_pipeline_example.create_dataset import write_dataset
from etl_pipeline_example.log_utils import setup_logging
from etl_pipeline_example.pipeline import run_pipeline

log = logging.getLogger(__name__)


def main():
    """Run the script from the command line."""
    setup_logging()
    if len(sys.argv) > 1:
        workdir = Path(sys.argv[1]).resolve()
        if not workdir.is_dir():
            raise SystemExit(f"Not a directory: {workdir}")
    else:
        workdir = Path(
            TemporaryDirectory(prefix="pipeline", suffix=".dir").name
        ).resolve()
    if (
        workdir.joinpath("company_returns.pkl").exists()
        and workdir.joinpath("market_returns.pkl").exists()
    ):
        log.debug("Company and market data already exists in %s", workdir)
    else:
        log.info("Creating data in %s...", workdir)
        write_dataset(workdir)
    run_pipeline(workdir)


if __name__ == "__main__":
    main()
