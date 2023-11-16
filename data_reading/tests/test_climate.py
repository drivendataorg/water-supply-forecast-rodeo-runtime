import pandas as pd

from wsfr_read.climate.cpc_outlooks import read_cpc_outlooks_precip, read_cpc_outlooks_temp


def test_read_cpc_outlooks():
    for reader_fn in (read_cpc_outlooks_precip, read_cpc_outlooks_temp):
        df = reader_fn("2021-03-15")
        assert df.shape[0] > 0
        assert (df.dtypes == "float64").all()
        assert df.index.dtypes.iloc[0] == "datetime64[ns]"
        assert (df.index.dtypes.iloc[1:] == "int64").all()
        assert (df.head(1).index.get_level_values("YEAR") == 2020).all()
        assert (df.head(1).index.get_level_values("MN") == 10).all()
        assert df.index.get_level_values("issue_date").max() < pd.to_datetime("2021-03-15")
