"""Code for downloading CPC Seasonal Outlooks, which are temperature and precipation forecasts
with up to 13 months lead time. Use the CLI to download, for example:

    python -m wsfr_download cpc_outlooks 2019 2021

or use the `bulk` command to download many sources at once based on a config file.

    python -m wsfr_download bulk data_download/hindcast_test_config.yml

You can also import this module and use it as a library.

See the challenge website for more about this approved data source:
https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/page/801/#grace-based-soil-moisture-and-groundwater-drought-indicators
"""

from functools import partial
from itertools import chain
import threading
from typing import Annotated

from loguru import logger
import requests
from tqdm.contrib.concurrent import thread_map
import typer

from wsfr_download.config import DATA_ROOT
from wsfr_download.utils import DownloadResult, log_download_results

CPC_OUTLOOKS_DIR = DATA_ROOT / "cpc_outlooks"

TEMP_URL_TEMPLATE = "https://www.cpc.ncep.noaa.gov/pacdir/NFORdir/HUGEdir2/cpcllftd.{year}.dat"
PRECIP_URL_TEMPLATE = "https://www.cpc.ncep.noaa.gov/pacdir/NFORdir/HUGEdir2/cpcllfpd.{year}.dat"


thread_local = threading.local()


def _get_session() -> requests.Session:
    """Utility for creating one requests session per thread."""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def download_cpc_outlooks_for_year(
    year: int, skip_existing: bool
) -> tuple[DownloadResult, DownloadResult]:
    """Download CPC temperature and precipitation outlook data files for a given calendar year."""
    session = _get_session()

    results = []
    for url_template, file_template in [
        (TEMP_URL_TEMPLATE, "cpcllftd.{year}.dat"),
        (PRECIP_URL_TEMPLATE, "cpcllfpd.{year}.dat"),
    ]:
        out_file = CPC_OUTLOOKS_DIR / file_template.format(year=year)
        if skip_existing and out_file.exists():
            results.append(DownloadResult.SKIPPED_EXISTING)
        else:
            resp = session.get(url_template.format(year=year))
            if resp.status_code == 200:
                with out_file.open("w") as fp:
                    fp.write(resp.text)
                results.append(DownloadResult.SUCCESS)
            elif resp.status_code == 404:
                results.append(DownloadResult.SKIPPED_NO_DATA)
            else:
                resp.raise_for_status()

    return tuple(results)


def download_cpc_outlooks(
    forecast_years: Annotated[list[int], typer.Argument(help="Forecast years to download for.")],
    skip_existing: Annotated[bool, typer.Option(help="Whether to skip an existing file.")] = True,
):
    """Download CPC seasonal outlooks from the CPC Outlooks Archive
    <https://www.cpc.ncep.noaa.gov/pacdir/NFORdir/HUGEdir2/outlook_files.html>.
    For each forecast year specified, that calendar year and the previous calendar year are
    downloaded.
    """
    logger.info(f"Downloading CPC Outlooks data for forecast years: {forecast_years}...")
    CPC_OUTLOOKS_DIR.mkdir(exist_ok=True, parents=True)

    years_to_download = sorted(set(chain.from_iterable((fy - 1, fy) for fy in forecast_years)))
    logger.info(f"Calendar years to download: {years_to_download}")

    download_results = list(
        thread_map(
            partial(download_cpc_outlooks_for_year, skip_existing=skip_existing),
            years_to_download,
            total=len(years_to_download),
            chunksize=1,
        )
    )
    temp_results, precip_results = zip(*download_results)

    log_download_results(temp_results, "[temp]")
    log_download_results(precip_results, "[precip]")
    logger.success("CPC Outlooks download complete.")
