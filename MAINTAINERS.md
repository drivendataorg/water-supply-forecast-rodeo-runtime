# Maintainer Notes

This file documents notes for maintainers of the repository.

## data_download

This directory contains a Python package `wsfr-download` that provides a CLI for downloading approved feature datasets. Package metadata, including abstract dependencies, are defined in the `pyproject.toml` file.

### Pinning requirements

In order to have a reproducible environment, we use pip-tools to pin dependencies. From the repository root, run

```bash
pip-compile data_download/pyproject.toml
```
