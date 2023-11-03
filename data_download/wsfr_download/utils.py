from functools import lru_cache

import geopandas as gpd
import pandas as pd

from wsfr_download.config import GEOSPATIAL_FILE, METADATA_FILE


@lru_cache
def site_metadata():
    """Load competition metadata.csv file with site metadata."""
    metadata_df = pd.read_csv(METADATA_FILE, index_col="site_id", dtype={"usgs_id": "string"})
    return metadata_df


@lru_cache
def site_geospatial(layer: str):
    """Load competition geospatial.gpkg file with site polygons."""
    return gpd.read_file(GEOSPATIAL_FILE, layer=layer)
