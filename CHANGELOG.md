# Changelog

## December 11, 2023

- Added the following R dependencies to the runtime image:
    - r-base v4.3.2
    - r-plyr v1.8.9
    - r-terra v1.7
    - r-sf v1.0
    - r-zoo v1.8
    - r-doparallel v1.0.17
    - r-foreach v1.5.2
    - r-iterators v1.0.14
    - r-nlme v3.1
    - r-mgcv v1.9
    - r-qgam v1.3.4
    - r-modelr v0.1.11
    - r-missmda v1.19
    - r-mvtnorm v1.2
    - r-mice v3.16.0
    - r-factominer v2.9
    - r-matrix v1.6
    - r-rcpp v1.0.11
    - r-dataretrieval v2.7.14
- Pins pystac-client v0.7.5

## December 10, 2023

- Added the following dependencies to the runtime image:
    - gdal v3.7.3 (was previously implicitly included, now pinned explicitly)
    - richdem v2.3.0

## December 9, 2023

- Added the following dependencies to the runtime image:
    - esda v2.5.1
    - giddy v2.3.4
    - libpysal v4.9.2
    - pointpats v2.4.0
    - spreg v1.4.2
- Added the following hosts to the firewall whitelist ([`allowed_hosts.txt`](./allowed_hosts.txt))
    - University of Arizona file server for [UA/SWANN](https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/page/801/#uaswann) (`climate.arizona.edu`)
    - Storage Account for [Copernicus DEM GLO-90](https://www.drivendata.org/competitions/254/reclamation-water-supply-forecast-dev/page/801/#copernicus-dem-glo-90) (`elevationeuwest.blob.core.windows.net`)

## December 8, 2023

- Added the following dependencies to the runtime image:
    - cdsapi v0.6.1
    - fiona v1.9.5
    - imbalanced-learn v0.11.0
    - mapie v0.7.0
    - pygrib v2.1.5
    - pykrige v1.7.1
    - pyproj v3.6.1
    - joblib v1.3.2
    - salem v0.3.9
    - shapely v2.0.2
    - xagg v0.3.1
- Fixed bug when building on Apple Silicon Macs by explicitly specifying `--platform=linux/amd64` in Dockerfile.

## December 6, 2023 (3)

- Fixed an issue that prevented quantile-forest from being correctly installed.

## December 6, 2023 (2)

- Added the following packages to the runtime environment:
  - catboost v1.2.2
  - quantile-forest v1.2.3

## December 6, 2023

- Fixed an issue during code execution where the mounted data drive appeared empty when scanning for files with `glob`, `listdir`, or `iterdir` due to a transient effect from mounting. The supervisor code now waits up to 30 seconds for scanning to return results before running your code.

## December 3, 2023

- Fixed bug in parsing CPC Precipitation Outlooks in `wsfr_read.climate.cpc_outlooks` to additionally handle case where 2006 also has a slightly different format.

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
