# Maintainer Notes

This file documents notes for maintainers of the repository.

## data_download

This directory contains a Python package `wsfr-download` that provides a CLI for downloading approved feature datasets. Package metadata, including abstract dependencies, are defined in the `pyproject.toml` file.

### Locking requirements

`pyproject.toml` declares the package and test dependencies; `uv.lock` pins the environment. From the repository root, run

```bash
uv lock --project data_download
uv sync --project data_download
```

Run the package tests with `uv run --project data_download pytest data_download/tests`.

## data_reading

This directory contains a Python package `wsfr-read` that provides sample code for reading data downloaded by `wsfr-download`. Package metadata, including abstract dependencies, are defined in the `pyproject.toml` file. This package will be included in the runtime container image and dependencies will be pinned as part of locking the runtime environment.

### Tests

You can run some basic integration tests with

```bash
pytest data_reading
```

This assumes you've downloaded all of the hindcast data, i.e., you've run `python -m wsfr_download bulk data_download/hindcast_test_config.yml`.
