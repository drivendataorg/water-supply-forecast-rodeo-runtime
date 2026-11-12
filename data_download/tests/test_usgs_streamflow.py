import pandas as pd

from wsfr_download import usgs_streamflow


def test_get_daily_values_for_usgs_site_uses_waterdata_and_preserves_shape(monkeypatch):
    response = pd.DataFrame(
        {
            "time": ["2020-10-01", "2020-10-02"],
            "value": [1.2, 2.3],
            "parameter_code": ["00060", "00060"],
        }
    )
    request = {}

    def fake_get_daily(**kwargs):
        request.update(kwargs)
        return response, None

    monkeypatch.setattr(usgs_streamflow.waterdata, "get_daily", fake_get_daily)

    result = usgs_streamflow.get_daily_values_for_usgs_site(
        usgs_id="12345678",
        forecast_year=2021,
        fy_start_month=10,
        fy_start_day=1,
        fy_end_month=7,
        fy_end_day=22,
    )

    assert request == {
        "monitoring_location_id": "USGS-12345678",
        "parameter_code": "00060",
        "statistic_id": "00003",
        "time": "2020-10-01/2021-07-21",
    }
    assert result.index.name == "datetime"
    assert list(result.columns) == ["00060_Mean"]
    assert result["00060_Mean"].tolist() == [1.2, 2.3]