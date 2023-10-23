import os
from loguru import logger
from pathlib import Path

DATA_ROOT = Path(os.getenv("WSFR_DATA_ROOT", Path.cwd() / "data"))

logger.info(f"DATA_ROOT is {DATA_ROOT}")
