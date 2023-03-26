[![Build](https://github.com/ilCatania/etl-pipeline-example/actions/workflows/python-build.yml/badge.svg)](https://github.com/ilCatania/etl-pipeline-example/actions/workflows/python-build.yml)
[![codecov](https://codecov.io/gh/ilCatania/etl-pipeline-example/branch/master/graph/badge.svg?token=S42VL6ZU57)](https://codecov.io/gh/ilCatania/etl-pipeline-example)

# etl-pipeline-example

ETL pipeline example.

## Installation

Install _etl-pipeline-example_ with `pip`:

```console
$ pip install etl-pipeline-example
```

then check that your installation is correct by importing the main module:

```console
$ python -c 'import etl_pipeline_example'
```

## Executing

To execute the pipeline, run the python module from the command line and provide
a directory where you have already created company and market returns data that
you would like to process. If you provide a directory without existing company
and market data, it will be created:

```bash
mkidr workdir
python -m etl_pipeline_example workdir
```

The resulting correlations will be saved in csv format in
`${workdir}/store/result_corr.csv`.

## Development

After creating your own virtual environment, install
_etl-pipeline-example_ for development by using editable mode and the
appropriate extras:

```console
$ python -m venv venv
$ source venv/bin/activate
$ pip install -e .[dev,tests]
```
