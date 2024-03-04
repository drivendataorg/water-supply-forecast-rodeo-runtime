"""Code for downloading the Pacific-North American (PNA) pattern index. Use the CLI to download,
for example:

    python -m wsfr_download pna

or use the `bulk` command to download many sources at once based on a config file.

    python -m wsfr_download bulk data_download/hindcast_test_config.yml

You can also import this module and use it as a library.

See the challenge website for more about this approved data source:
https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/page/801/#pacific-north-american-pna-index
"""

from typing import Annotated

from loguru import logger
import requests
import typer

from wsfr_download.config import DATA_ROOT

SOURCE_URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/pna/norm.pna.monthly.b5001.current.ascii.table"
FILE_PATH_PARTS = ("teleconnections", "pna.txt")


def download_pna(
    skip_existing: Annotated[bool, typer.Option(help="Whether to skip an existing file.")] = True,
):
    """Download Pacific-North American Index data."""
    logger.info("Downloading PNA Index data...")
    response = requests.get(SOURCE_URL)
    response.raise_for_status()
    out_file = DATA_ROOT.joinpath(*FILE_PATH_PARTS)
    logger.info(f"Output file path is {out_file}")
    if skip_existing and out_file.exists():
        logger.info("File exists. Skipping.")
    else:
        out_file.parent.mkdir(exist_ok=True, parents=True)
        with out_file.open("w") as fp:
            fp.write(response.text)
        logger.info("Data downloaded to file.")
    logger.success("PNA Index download complete.")
