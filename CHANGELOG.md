# Changelog

## November 21, 2023

- Added `requests.exceptions.ConnectionError` to the retry conditions for `wsfr_download.cdec.download_station_data`

## November 17, 2023

- Added data download code for CDEC snow monitoring stations (`cdec`). This depends on the `cdec_snow_stations.csv` file added to the [data download page](https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/data/).
- Added data download and data reading code for CPC Seasonal Outlooks (`cpc_outlooks`). An additional file `cpc_climate_divisions.gpkg` file has been added to the [data download page](https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/data/).
- Added retries to the SNOTEL station download code to improve reliability
- Added `skip_existing` option to all download functions to control whether files that already exist should be skipped. All download functions now skip existing files by default. To force downloads to overwrite existing files, set this to `false` in the bulk download config file when using `bulk`, or use the `--no-skip-existing` flag when using individual data source download commands.

## November 9, 2023

- Added instructions to README about the data directory.

## November 8, 2023

- Added missing geopandas dependency to `wsfr-download`

## November 7, 2023

Data download additions:

- USGS daily streamflow for challenge forecast sites (`usgs_streamflow`)
- SNOTEL snowpack station measurements (`snotel`)
- SNODAS snowpack properties estimates (`snodas`)
- Palmer Drought Severity Index (PDSI) from gridMET (`pdsi`)

Data reading additions:

- USGS daily streamflow

## November 1, 2023

Initial release with:

- Download support for GRACE drought indicators and teleconnection indices.
- Read support for teleconneciton indices
