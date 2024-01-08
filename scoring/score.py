"""
Water Supply Forecast Rodeo Scoring Client

This script calculates two metrics, Averaged Mean Quantile Loss and Internal Coverage, for the
Water Supply Forecast Rodeo competition. For more information on the metrics, refer to the
competition's problem description:
https://www.drivendata.org/competitions/259/reclamation-water-supply-forecast/page/827/#performance-metric

Usage:
    python score.py forecast_train.csv submission.csv

Arguments:
    - true_values: Path to the ground truth labels CSV file with columns "site_id", "year", and
      "volume" (e.g., 'forecast_train.csv' on the Data Download page).
    - predicted_values: Path to the correctly formatted submission CSV file with columns "site_id",
      "issue_date", "volume_10", "volume_50", and "volume_90".

Author:
    DrivenData

Date:
    January 8, 2024
"""


import argparse
import json

import numpy as np
import pandas as pd
from sklearn.metrics import mean_pinball_loss


def averaged_mean_quantile_loss(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Calculates averaged mean quantile loss for quantile predictions.

    Args:
        actual (np.ndarray): Array of actual values (labels)
        predicted (np.ndarray): Array of predicted values

    Returns:
        float: Averaged mean quantile loss
    """
    quantiles = [0.10, 0.50, 0.90]
    per_quantile_loss = []
    for idx, quantile in enumerate(quantiles):
        # Multiply by 2 so that 0.50 quantile is equivalent to MAE
        per_quantile_loss.append(
            2 * mean_pinball_loss(y_true=actual, y_pred=predicted[:, idx], alpha=quantile)
        )
    return np.average(per_quantile_loss)


def interval_coverage(actual: np.ndarray, predicted: np.ndarray) -> float:
    """Calculates interval coverage for quantile predictions. Assumes at least two columns
    in `predicted`, and that the first column is the lower bound of the interval, and the last
    column is the upper bound of the interval.

    Args:
        actual (np.ndarray): Array of actual values (labels)
        predicted (np.ndarray): Array of predicted values

    Returns:
        float: Interval coverage (proportion of predictions that fall within lower and upper bound)
    """
    # Use ravel to reshape to 1D arrays.
    lower = predicted[:, 0].ravel()
    upper = predicted[:, -1].ravel()
    actual = actual.ravel()
    return np.average((lower <= actual) & (actual <= upper))


def validate(actual: pd.DataFrame, predicted: pd.DataFrame) -> None:
    """Checks that the predicted and actual dataframes have correct columns and indices

    Args:
        actual (pd.DataFrame): Dataframe of actual values (labels)
        predicted (pd.DataFrame): Dataframe of predicted values
    """
    # check site ids match
    actual_sites = set(actual.site_id)
    predicted_sites = set(predicted.site_id)
    assert (
        actual_sites == predicted_sites
    ), f"Actual and predicted site IDs do not match. Different sites: {actual_sites.symmetric_difference(predicted_sites)}"
    # check years match
    actual_years = set(actual.year)
    predicted_years = set(pd.to_datetime(predicted.issue_date).dt.year)
    assert (
        actual_years == predicted_years
    ), f"Actual and predicted years do not match. Different years: {actual_years.symmetric_difference(predicted_years)}"
    # check for duplicates
    assert (
        not predicted[["site_id", "issue_date"]].duplicated().any()
    ), f"Duplicate entries found in predictions for the combination of 'site_id' and 'issue_date'."
    assert (
        not actual[["site_id", "year"]].duplicated().any()
    ), f"Duplicate entries found in labels for the combination of 'site_id' and 'year'."
    # check for columns names
    assert (
        predicted.columns == ["site_id", "issue_date", "volume_10", "volume_50", "volume_90"]
    ).all(), "Found error in predicted column names. Columns should be: ['site_id', 'issue_date', 'volume_10', 'volume_50', 'volume_90']"
    assert (
        actual.columns == ["site_id", "year", "volume"]
    ).all(), "Found error in label column names. Columns should be: ['site_id', 'year', 'volume']"


def main():
    """Load CSV files and score predictions."""
    parser = argparse.ArgumentParser(
        description="Calculate averaged mean quantile loss and interval coverage."
    )
    parser.add_argument("true_values", type=str, help="Path to the true values CSV file")
    parser.add_argument("predicted_values", type=str, help="Path to the predicted values CSV file")

    args = parser.parse_args()

    # read and validate
    actual = pd.read_csv(args.true_values)
    predicted = pd.read_csv(args.predicted_values)
    validate(actual, predicted)
    # merge
    predicted["year"] = pd.to_datetime(predicted["issue_date"]).dt.year
    merged = actual.merge(predicted, on=["site_id", "year"], how="right")
    # score
    scores = {
        "averaged_mean_quantile_loss": averaged_mean_quantile_loss(
            merged[["volume"]].values, merged[["volume_10", "volume_50", "volume_90"]].values
        ),
        "interval_coverage": interval_coverage(
            merged[["volume"]].values, merged[["volume_10", "volume_50", "volume_90"]].values
        ),
    }
    print(json.dumps(scores, indent=2))
    return scores


if __name__ == "__main__":
    main()
