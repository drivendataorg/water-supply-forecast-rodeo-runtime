import os
import loguru
from pathlib import Path

DATA_ROOT = Path(os.getenv("WSFR_DATA_ROOT", Path.cwd() / "data")).resolve()
