from pathlib import Path
from typing import Annotated, Any

from loguru import logger
from pydantic import BaseModel, Field, field_validator
import typer
from yaml import safe_load

from wsfr_download.cdec import download_cdec
from wsfr_download.config import DATA_ROOT
from wsfr_download.cpc_outlooks import download_cpc_outlooks
from wsfr_download.grace_indicators import download_grace_indicators
from wsfr_download.mjo import download_mjo
from wsfr_download.modis_vegetation import download_modis_vegetation
from wsfr_download.nino_regions_sst import download_nino_regions_sst
from wsfr_download.oni import download_oni
from wsfr_download.pdo import download_pdo
from wsfr_download.pdsi import download_pdsi
from wsfr_download.pna import download_pna
from wsfr_download.snodas import download_snodas
from wsfr_download.snotel import download_snotel
from wsfr_download.soi import download_soi
from wsfr_download.usgs_streamflow import download_usgs_streamflow

app = typer.Typer(
    help=(
        "CLI program for downloading feature data for the Water Supply Forecast Rodeo competition "
        "on DrivenData. Available commands are listed below. Use '--help' for an individual "
        "command for details on how to use."
    )
)


@app.callback()
def data_root_callback():
    """Callback to log location of data root directory."""
    logger.info(f"DATA_ROOT is {DATA_ROOT}")


# Registry mapping data source keywords to respective download functions
DATA_SOURCE_TO_FUNCTION = {
    "cdec": download_cdec,
    "usgs_streamflow": download_usgs_streamflow,
    "cpc_outlooks": download_cpc_outlooks,
    "grace_indicators": download_grace_indicators,
    "mjo": download_mjo,
    "modis_vegetation": download_modis_vegetation,
    "nino_regions_sst": download_nino_regions_sst,
    "oni": download_oni,
    "pdo": download_pdo,
    "pdsi": download_pdsi,
    "pna": download_pna,
    "snodas": download_snodas,
    "snotel": download_snotel,
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
    """Data model for YAML-based configuration file for the `bulk` download command.

    When provided, the end date defined by `fy_end_month` and `fy_end_day` is exclusive,
    i.e., for Jan 1, the most recent data will be for Dec 31.
    """

    forecast_years: list[int]
    fy_start_month: int | None = None
    fy_start_day: int | None = None
    fy_end_month: int | None = None
    fy_end_day: int | None = None
    skip_existing: bool = True
    data_sources: list[DataSourceConfig] = Field(default_factory=list, validate_default=True)

    @field_validator("data_sources")
    @classmethod
    def data_sources_must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("data_sources must not be empty")
        return v


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
            # take union of bulk config and data source config args;
            # latter overrides former for key collisions.
            kwargs = (
                bulk_config.model_dump(exclude="data_sources", exclude_none=True)
                | data_source_config.kwargs
            )
            fn(**kwargs)
        except TypeError as exc:
            if "got an unexpected keyword argument" in str(exc):
                try:
                    # CPC outlooks download does not require fy start and end parameters
                    fn(
                        forecast_years=bulk_config.forecast_years,
                        skip_existing=bulk_config.skip_existing,
                        **data_source_config.kwargs,
                    )
                except TypeError as exc:
                    if "got an unexpected keyword argument" in str(exc):
                        # Teleconnection download function that doesn't require specifying years
                        fn(skip_existing=bulk_config.skip_existing, **data_source_config.kwargs)
                    else:
                        raise
            else:
                raise
    logger.success("Bulk download complete.")


if __name__ == "__main__":
    app()
