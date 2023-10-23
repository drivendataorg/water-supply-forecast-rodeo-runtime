"""Code for downloading Madden-Julian Oscillation (MJO) pentad indices. Use the CLI to download,
for example:

    python -m wsfr_download mjo

or use the `bulk` command to download many sources at once based on a config file.

    python -m wsfr_download bulk data_download/hindcast_test_config.yml

You can also import this module and use it as a library.

See the challenge website for more about this approved data source:
https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/page/801/#madden-julian-oscillation-mjo-pentad-indices
"""

from loguru import logger
import requests

from wsfr_download.config import DATA_ROOT

SOURCE_URL = "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_mjo_index/proj_norm_order.ascii"
FILE_PATH_PARTS = ("teleconnections", "mjo.txt")


def download_mjo():
    """Download Madden-Julian Oscillation indices."""
    logger.info("Downloading MJO data...")
    response = requests.get(SOURCE_URL)
    out_file = DATA_ROOT.joinpath(*FILE_PATH_PARTS)
    out_file.parent.mkdir(exist_ok=True, parents=True)
    with out_file.open("w") as fp:
        fp.write(response.text)
    logger.success(f"MJO data written to {out_file}")
