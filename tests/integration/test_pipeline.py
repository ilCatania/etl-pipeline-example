from filecmp import cmp

from etl_pipeline_example.create_dataset import write_dataset
from etl_pipeline_example.pipeline import run_pipeline


def test_pipeline_small_dataset(tmp_path, testfiles):
    expected = testfiles / "expected_results_small.csv"
    write_dataset(tmp_path, "2016-01-01", n_companies=6, n_dates=1000)
    run_pipeline(tmp_path)
    actual = tmp_path.joinpath("store/result_corr.csv")
    assert cmp(actual, expected), "Files are different!"
