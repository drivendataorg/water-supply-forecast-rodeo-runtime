import datetime
from unittest.mock import Mock

from netCDF4 import Dataset
import pandas as pd

from wsfr_download import cdec, pdsi, snotel
from wsfr_download.utils import DownloadResult


def test_cdec_download_sensor_data_writes_records_without_false_no_data(tmp_path):
    response = Mock()
    response.json.return_value = [
        {"stationId": "ABC", "SENSOR_NUM": 3, "date": "2020-10-01", "value": 12.5},
        {"stationId": "ABC", "SENSOR_NUM": 18, "date": "2020-10-01", "value": 4.0},
    ]
    session = Mock()
    session.get.return_value = response

    results = cdec.download_sensor_data(
        station_ids=["ABC"],
        start_date=datetime.date(2020, 10, 1),
        end_date=datetime.date(2020, 10, 2),
        out_dir=tmp_path,
        skip_existing=False,
        sensor_numbers=(3, 18),
        session=session,
    )

    saved = pd.read_csv(tmp_path / "ABC.csv")
    assert results == [DownloadResult.SUCCESS]
    assert saved["stationId"].tolist() == ["ABC", "ABC"]
    assert saved["SENSOR_NUM"].tolist() == [3, 18]
    assert saved["value"].tolist() == [12.5, 4.0]


def test_snotel_data_pull_builds_nullable_numeric_series(monkeypatch):
    response = Mock()
    response.json.return_value = [
        {
            "data": [
                {
                    "stationElement": {"elementCode": "WTEQ", "durationName": "DAILY"},
                    "values": [
                        {"date": "2020-10-01", "value": 1.25},
                        {"date": "2020-10-02"},
                    ],
                }
            ]
        }
    ]
    session = Mock()
    session.get.return_value = response
    monkeypatch.setattr(snotel, "_get_session", lambda: session)

    result = snotel.get_data_for_station(
        "123:CO:SNTL",
        datetime.date(2020, 10, 1),
        datetime.date(2020, 10, 2),
    )

    assert result.index.name == "date"
    assert result.index.tolist() == ["2020-10-01", "2020-10-02"]
    assert str(result["WTEQ_DAILY"].dtype) == "Float64"
    assert result["WTEQ_DAILY"].tolist() == [1.25, pd.NA]
    session.get.assert_called_once()
    assert "stationTriplets=123:CO:SNTL" in session.get.call_args.args[0]


def test_pdsi_netcdf_validation_handles_numpy_backed_day_values(tmp_path):
    file_path = tmp_path / "pdsi.nc"
    with Dataset(file_path, "w") as dataset:
        dataset.createDimension("time", 2)
        dataset.createDimension("latitude", 1)
        dataset.createDimension("longitude", 1)
        dataset.createVariable("day", "i4", ("time",))[:] = [0, 1]
        dataset.createVariable("lat", "f4", ("latitude",))[:] = [40.0]
        dataset.createVariable("lon", "f4", ("longitude",))[:] = [-110.0]
        dataset.createVariable(
            "daily_mean_palmer_drought_severity_index",
            "f4",
            ("time", "latitude", "longitude"),
        )[:] = 0.0

    assert pdsi.validate_netcdf4(file_path, fy_start_month=10, fy_end_month=7)