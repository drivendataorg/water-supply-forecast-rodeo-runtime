# Changelog

## December 1, 2023

- Fixed bug in parsing CPC Precipitation Outlooks in `wsfr_read.climate.cpc_outlooks`. This now handles the case where the 2004 file has a slightly different format.

## November 29, 2023

- Added runtime items to the repository.
    - Runtime image specification in [`runtime/`](./runtime/)
    - Example submission and submission template in [`examples/`](./examples/)
    - Documentation added to [README](./README.md#testing-a-submission-locally).
    - Firewall allowed hosts documented in [`allowed_hosts.txt`](./allowed_hosts.txt). See [documentation](./README.md/#runtime-network-access).
- Added data directory manifest ([`data.find.txt`](./data.find.txt)) and disk usage ([`data.du.txt`](./data.du.txt)). See [documentation](./README.md/#expected-files).
- Added `read_test_monthly_naturalized_flow` function to `data_reading` that loads the monthly naturalized flow time series for a particular site and issue date.

## November 27, 2023

- Added data download code for MODIS vegetation indices (`modis_vegetation`).
- Changed `cdec` download code for improved reliability. It now downloads data for large batches of stations (instead of individually) in order to reduce the number of network calls to CDEC servers.

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
