import os
from pathlib import Path

from loguru import logger
from tqdm import tqdm

# Configure loguru logger with tqdm.write
# https://github.com/Delgan/loguru/issues/135
logger.remove(0)
logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)


DATA_ROOT = Path(os.getenv("WSFR_DATA_ROOT", Path.cwd() / "data"))
METADATA_FILE = DATA_ROOT / "metadata.csv"
GEOSPATIAL_FILE = DATA_ROOT / "geospatial.gpkg"

logger.info(f"DATA_ROOT is {DATA_ROOT}")
