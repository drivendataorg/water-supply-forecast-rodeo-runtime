from functools import lru_cache

import pandas as pd

from wsfr_read.config import METADATA_FILE


@lru_cache
def read_metadata() -> pd.DataFrame:
    """Load competition metadata.csv file with site metadata."""
    metadata_df = pd.read_csv(METADATA_FILE, index_col="site_id", dtype={"usgs_id": "string"})
    return metadata_df
