from collections.abc import Hashable
import json
from pathlib import Path
from typing import Any

from loguru import logger
import pandas as pd

from wsfr_read.streamflow import read_test_monthly_naturalized_flow
from wsfr_read.sites import read_metadata


def preprocess(src_dir: Path, data_dir: Path, preprocessed_dir: Path) -> dict[Hashable, Any]:
    # Load parameters for sites that don't have monthly naturalized flow data
    train_stats_path = src_dir / "train_stats.json"
    logger.info("Loading training years mean and std for each site from " + str(train_stats_path))
    with train_stats_path.open("r") as fp:
        train_stats = json.load(fp)
    return {"train_stats": train_stats}


def predict(
    site_id: str,
    issue_date: str,
    assets: dict[Any, Any],
    src_dir: Path,
    data_dir: Path,
    preprocessed_dir: Path,
) -> tuple[float, float, float]:
    forecast_year = pd.to_datetime(issue_date).year

    # Get season start and end for this site
    site_metadata_df = read_metadata()
    season_start_month, season_end_month = site_metadata_df.loc[
        site_id, ["season_start_month", "season_end_month"]
    ]

    try:
        # Load monthly naturalized flow time series for test years
        monthly_flow_df = read_test_monthly_naturalized_flow(
            site_id=site_id, issue_date=issue_date
        )

        # Create timestamp index for convenience. Will be first of each month, e.g., 2021-01-01
        monthly_flow_df = monthly_flow_df.reset_index()
        monthly_flow_df["timestamp"] = pd.to_datetime(
            monthly_flow_df.year.astype(str) + "-" + monthly_flow_df.month.astype(str),
            format="%Y-%m",
        )
        monthly_flow_df = monthly_flow_df.set_index("timestamp")

        monthly_volume_ser = monthly_flow_df["volume"]
    except KeyError:
        # No data for this site
        monthly_volume_ser = pd.Series([])

    # If there is no data: no rows or rows are all NA, then use mean and std of seasonal water
    # supply from training years to calculate predictions
    if monthly_volume_ser.empty or monthly_volume_ser.isna().all():
        # Get mean and std that was calculated from training data and bundled with submission
        # and return those as the prediction
        train_stats = assets["train_stats"][site_id]
        mean, std = train_stats["mean"], train_stats["std"]
        ci = std * 1.281552  # z-score for 80% centered confidence interval for normal distribution
        return mean - ci, mean, mean + ci

    # Reindex to add rows through end of season
    pred_monthly_volume_ser = monthly_volume_ser.reindex(
        pd.date_range(
            monthly_volume_ser.index[0],
            f"{forecast_year}-{season_end_month:02}",
            freq="MS",
        )
    )

    # fill missing values forward and then calculate exponentially weighted moving average
    pred_monthly_volume_ser = pred_monthly_volume_ser.fillna(
        pred_monthly_volume_ser.ffill().ewm(com=0.5).mean()
    )

    # cumulative volume for forecast season
    seasonal_volume = pred_monthly_volume_ser.loc[
        pd.date_range(
            f"{forecast_year}-{season_start_month:02}",
            f"{forecast_year}-{season_end_month:02}",
            freq="MS",
        )
    ].sum()

    # confidence interval
    # get z-score for 80% centered confidence interval for normal distribution for one month
    # multiply by number of months in forecast season
    std = monthly_volume_ser.std()
    if pd.isna(std):
        std = monthly_volume_ser.std(ddof=0)
    seasonal_ci = (season_end_month - season_start_month + 1) * std * 1.281552

    return seasonal_volume - seasonal_ci, seasonal_volume, seasonal_volume + seasonal_ci
