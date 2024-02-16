import json
from pathlib import Path
from typing import Hashable, Any

from loguru import logger
from tqdm import tqdm

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_pinball_loss
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.ensemble import GradientBoostingRegressor

WORKING_DIR = Path(__file__).parent
DATA_DIR = WORKING_DIR.parent / "data"
MODELS_DIR = WORKING_DIR / "models"
MODELS_DIR.mkdir(exist_ok=True)


# Metrics are copied from https://github.com/drivendataorg/water-supply-forecast-rodeo-runtime/blob/main/scoring/score.py
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


def preprocess(src_dir: Path, data_dir: Path, preprocessed_dir: Path) -> dict[Hashable, Any]:
    """This is an example of a preprocess function you may have written from the Hindcast and
    Forecast Stage submissions. This example preprocesses a "mean monthly naturalized flow"
    feature. Import your own preprocess function, add your own precalculation code, or
    skip using this.
    """

    ## Precalculate your features here ##

    # Example feature: mean monthly naturalized flow
    mnf = pd.read_csv(data_dir / "cross_validation_monthly_flow.csv")
    sf = pd.read_csv(data_dir / "cross_validation_submission_format.csv")
    sf["issue_date"] = pd.to_datetime(sf["issue_date"])
    merged = sf.merge(
        mnf,
        left_on=[sf.site_id, sf.issue_date.dt.year],
        right_on=["site_id", "forecast_year"],
        how="left",
    )

    # Filter to flow data that occurs before the `issue_date` in the same `forecast_year`
    filtered_df = merged[
        (merged["year"] < merged["issue_date"].dt.year)
        | (
            (merged["year"] == merged["issue_date"].dt.year)
            & (merged["month"] < merged["issue_date"].dt.month)
        )
    ]
    # Get mean flow volume for the same water year up until the `issue_date`
    mean_volume = filtered_df.groupby(["site_id", "issue_date"]).volume.mean().reset_index()
    mean_volume["month"] = mean_volume.issue_date.dt.month

    return {"mean_volume": mean_volume}


def main():
    """This main function performs year-wise leave-one-out cross-validation over the 20-year
    Hindcast period. It trains a new model for every fold (one water year as test) using
    precalculated features. Finally, it generates a correctly formatted submission csv.
    """
    # Load data. These files can be found on the Final Stage data download page
    # https://www.drivendata.org/competitions/262/reclamation-water-supply-forecast-final/data/
    labels = pd.read_csv(DATA_DIR / "cross_validation_labels.csv")
    submission_format = pd.read_csv(DATA_DIR / "cross_validation_submission_format.csv")
    submission_format.issue_date = pd.to_datetime(submission_format.issue_date)

    # Merge submission_format with labels
    INDEX = ["site_id", "issue_date"]
    labels = submission_format.merge(
        labels,
        left_on=["site_id", submission_format.issue_date.dt.year],
        right_on=["site_id", "year"],
        how="left",
    ).set_index(INDEX)

    # Fetch precalcuated features from assets
    assets = preprocess(WORKING_DIR, DATA_DIR, None)
    features = assets["mean_volume"].set_index(INDEX)
    feature_cols = features.columns

    #### CROSS-VALIDATION ####

    # Initialize Leave-One-Group-Out cross-validator
    # and a dictionary to keep track of scores for each fold
    logo = LeaveOneGroupOut()
    scores = {}

    # Keep track of predictions generated for each fold to construct the final submission
    all_preds = []

    # Perform Leave-One-Group-Out cross-validation
    for train_indices, test_indices in logo.split(labels.volume.values, groups=labels.year):
        # Split labels into train and test
        train_labels, test_labels = labels.iloc[train_indices], labels.iloc[test_indices]
        assert test_labels.year.nunique() == 1
        year = test_labels.year.iloc[0]
        logger.info(year)

        ## Loop over sites. Here we train a model per site.
        # You may not need to do this if you have a single model for all sites.
        # Or you may want to loop over issue dates if you train models per issue dates.

        # Keep track of predictions for each site.
        site_dfs = []
        for site in tqdm(test_labels.index.get_level_values("site_id").unique()):
            # Generate train and test sets for the given site
            site_train_mask = train_labels.index.get_level_values("site_id") == site
            site_train_y = train_labels[site_train_mask].copy()
            site_train_X = site_train_y.merge(
                features, how="left", left_index=True, right_index=True, suffixes=("_labels", None)
            )[feature_cols].fillna(-1)

            site_test_mask = test_labels.index.get_level_values("site_id") == site
            site_test_y = test_labels[site_test_mask].copy()
            site_test_X = site_test_y.merge(
                features, how="left", left_index=True, right_index=True, suffixes=("_labels", None)
            )[feature_cols].fillna(-1)

            # Train a model and generate predictions for each quantile
            site_preds = []
            for quantile in [0.1, 0.5, 0.9]:
                ## TRAIN
                model = GradientBoostingRegressor(
                    loss="quantile", alpha=quantile, random_state=8
                ).fit(site_train_X.values, site_train_y.volume.values)

                ## SAVE MODELS
                joblib.dump(model, MODELS_DIR / f"{site}-{year}-{quantile}.joblib")

                ## TEST
                site_preds.append(model.predict(site_test_X.values))

            # Cache predictions as a dataframe
            site_df = pd.DataFrame(
                np.column_stack(site_preds),
                columns=submission_format.set_index(INDEX).columns,
                index=site_test_X.index,
            )
            site_dfs.append(site_df)

        # Concat and reorder all predictions
        fold_preds = pd.concat(site_dfs).loc[test_labels.index]
        all_preds.append(fold_preds)

        # Calculate scores
        amql = averaged_mean_quantile_loss(test_labels.volume.values, fold_preds.values)
        ic = interval_coverage(test_labels.volume.values, fold_preds.values)
        scores[str(year)] = {"averaged_mean_quantile_loss": amql, "interval_coverage": ic}

    for year, d in scores.items():
        logger.info(
            f"{year} - amql: {d['averaged_mean_quantile_loss']} / " f"ic: {d['interval_coverage']}"
        )

    # You can write scores out to analyze later. Feel free to save additional information,
    # e.g. performance for each site
    scores_path = WORKING_DIR / "cv_scores.json"
    logger.info("Writing scores to {}", scores_path)
    scores_path.write_text(json.dumps(scores, indent=2))

    # Generate submission using submission format and write to CSV
    submission_format.set_index(INDEX, inplace=True)
    submission = pd.concat(all_preds).loc[submission_format.index]
    assert submission.shape == submission_format.shape
    assert (submission.columns == submission_format.columns).all()
    assert (submission.index == submission_format.index).all()
    out_path = WORKING_DIR / "cv_submission.csv"
    logger.info("Writing CV predictions to {}", out_path)
    submission.to_csv(out_path)

    # Get overall scores (same as averaging across folds)
    amql = averaged_mean_quantile_loss(labels.volume.values, submission.values)
    ic = interval_coverage(labels.volume.values, submission.values)
    logger.info(f"overall - amql: {amql} / " f"ic: {ic}")


if __name__ == "__main__":
    main()
