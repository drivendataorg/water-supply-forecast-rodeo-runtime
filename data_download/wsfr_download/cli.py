from pathlib import Path
from typing import Annotated, Any

from loguru import logger
from pydantic import BaseModel, Field, field_validator
from yaml import safe_load
import typer

from wsfr_download.grace_indicators import download_grace_indicators
from wsfr_download.mjo import download_mjo
from wsfr_download.nino_regions_sst import download_nino_regions_sst
from wsfr_download.oni import download_oni
from wsfr_download.pdo import download_pdo
from wsfr_download.pna import download_pna
from wsfr_download.soi import download_soi

app = typer.Typer(
    help=(
        "CLI program for downloading feature data for the Water Supply Forecast Rodeo competition "
        "on DrivenData. Available commands are listed below. Use '--help' for an individual "
        "command for details on how to use."
    )
)

# Registry mapping data source keywords to respective download functions
DATA_SOURCE_TO_FUNCTION = {
    "grace_indicators": download_grace_indicators,
    "mjo": download_mjo,
    "nino_regions_sst": download_nino_regions_sst,
    "oni": download_oni,
    "pdo": download_pdo,
    "pna": download_pna,
    "soi": download_soi,
}

for name, fn in DATA_SOURCE_TO_FUNCTION.items():
    # Register each data source download function as typer command
    app.command(name)(fn)


class DataSourceConfig(BaseModel):
    name: str
    kwargs: Annotated[dict[str, Any], Field(default_factory=dict)]

    @field_validator("name")
    @classmethod
    def name_is_valid(cls, v: str) -> str:
        if v not in DATA_SOURCE_TO_FUNCTION:
            raise ValueError(f"Unknown data source name: {v}")
        return v


class BulkConfig(BaseModel):
    """Data model for YAML-based configuration file for the `bulk` download command."""

    forecast_years: list[int]
    data_sources: list[DataSourceConfig]


@app.command()
def bulk(config: Annotated[Path, typer.Argument(help="Path to config file.")]):
    """Download many data sources as specified in a config file. The config file should be a YAML
    file. See the Pydantic model wsfr_download.cli.BulkConfig for the expected schema, and
    the hindcast_test_config.yml file for an example.

    For each data source, see the `download_*` function in the respective module to see available
    keyword arguments that you can override with "kwargs". For example, to download the GRACE
    indicators dataset with an earlier start month, you can do:

    \b
        data_sources:
        - name: grace_indicators
          kwargs: {'fy_start_month': 9}
    """
    logger.info(f"Bulk downloading data sources from config file: {config}")
    with config.open("r") as fp:
        bulk_config = BulkConfig.model_validate(safe_load(fp))
    for data_source_config in bulk_config.data_sources:
        fn = DATA_SOURCE_TO_FUNCTION[data_source_config.name]
        try:
            fn(forecast_years=bulk_config.forecast_years, **data_source_config.kwargs)
        except TypeError:
            fn(**data_source_config.kwargs)
    logger.success("Bulk download complete.")


if __name__ == "__main__":
    app()
