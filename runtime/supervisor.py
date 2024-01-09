from enum import Enum
import os
from pathlib import Path
from time import sleep
from typing import Any

from loguru import logger
import pandas as pd
from tqdm import tqdm

import wsfr_read.config

FORECAST_ISSUE_DATE = os.getenv("FORECAST_ISSUE_DATE")
IS_SMOKE = bool(os.getenv("IS_SMOKE", ""))

src_directory = Path("/code_execution/src")
data_directory = Path("/code_execution/data")
preprocessed_directory = Path("/code_execution/preprocessed")

# Add log handler for serializing event logs
logger.add(
    "/code_execution/submission/events.log",
    filter=lambda record: record["extra"].get("event"),
    serialize=True,
)


class Event(str, Enum):
    """Events tracked by structured logs."""

    MAIN_START = "main_start"
    MAIN_END = "main_end"
    IMPORT_START = "import_start"
    IMPORT_END = "import_end"
    PREPROCESS_START = "preprocess_start"
    PREPROCESS_END = "preprocess_end"
    PREDICT_START = "predict_start"
    PREDICT_END = "predict_end"


def validate_prediction(predictions: Any):
    try:
        assert len(predictions) == 3
        assert all(pd.api.types.is_float(y) for y in predictions)
        assert all(not pd.isnull(y) for y in predictions)
    except AssertionError:
        logger.error(f"Validation failed for predictions {predictions}")
        raise


def main():
    logger.info("Beginning code execution...", event=Event.MAIN_START)

    if FORECAST_ISSUE_DATE:
        logger.info("FORECAST_ISSUE_DATE: {}", FORECAST_ISSUE_DATE)
    logger.info("IS_SMOKE: {}", IS_SMOKE)
    logger.info("src_directory: {}", src_directory)
    logger.info("data_directory: {}", data_directory)
    logger.info("preprocessed_directory: {}", preprocessed_directory)

    # Check that DATA_ROOT is consistent
    assert wsfr_read.config.DATA_ROOT == data_directory

    # Check that data drive is fully mounted so that scanning returns data
    for i in range(30):
        try:
            next(data_directory.iterdir())
        except StopIteration:
            sleep(1)
        else:
            break
    try:
        next(data_directory.iterdir())
    except StopIteration:
        logger.error("Data directory not properly mounted after waiting 30 seconds.")
        raise
    else:
        logger.info("data_directory.iterdir returned results after waiting {} seconds.", i)

    # Create preprocessed directory
    try:
        preprocessed_directory.mkdir(parents=True)
    except FileExistsError:
        try:
            next(preprocessed_directory.iterdir())
            logger.warning(f"{preprocessed_directory} is not empty. This is not a clean run.")
        except StopIteration:
            pass

    logger.info("Importing src.solution.", event=Event.IMPORT_START)
    import src.solution

    assert hasattr(src.solution, "predict"), "Your solution.py must have a 'predict' function."

    logger.success("src.solution imported.", event=Event.IMPORT_END)

    if hasattr(src.solution, "preprocess"):
        logger.info("Running function 'preprocess'", event=Event.PREPROCESS_START)
        assets = src.solution.preprocess(
            src_dir=src_directory,
            data_dir=data_directory,
            preprocessed_dir=preprocessed_directory,
        )
        logger.success("preprocess complete", event=Event.PREPROCESS_END)
        logger.info(
            "Loaded assets with keys: {}",
            ", ".join(repr(k) for k in assets.keys()),
        )
    else:
        logger.info("No 'preprocess' function found in solution.py. Skipping...")
        assets = {}

    if IS_SMOKE:
        submission_format_path = data_directory / "smoke_submission_format.csv"
    else:
        submission_format_path = data_directory / "submission_format.csv"
    submission_format_df = pd.read_csv(submission_format_path, index_col=["site_id", "issue_date"])

    logger.info("Beginning predictions...", event=Event.PREDICT_START)
    update_iters = min(100, int(submission_format_df.shape[0] / 10))
    with open(os.devnull, "w") as devnull:
        pbar = tqdm(
            enumerate(submission_format_df.itertuples()),
            total=submission_format_df.shape[0],
            miniters=update_iters,
            file=devnull,
        )
        for i, row in pbar:
            if (i % update_iters) == 0:
                logger.info(str(pbar))
            site_id, issue_date = row.Index
            try:
                prediction = src.solution.predict(
                    site_id=site_id,
                    issue_date=issue_date,
                    assets=assets,
                    src_dir=src_directory,
                    data_dir=data_directory,
                    preprocessed_dir=preprocessed_directory,
                )

                validate_prediction(prediction)

                submission_format_df.loc[
                    row.Index, ["volume_10", "volume_50", "volume_90"]
                ] = prediction

            except Exception as exc:
                logger.error("Error predicting {}", row.Index)
                raise exc

    logger.success("Predictions complete.", event=Event.PREDICT_END)

    logger.info("Saving predictions to file")
    submission_format_df.to_csv("/code_execution/submission/submission.csv")

    logger.success("Code execution run complete.", event=Event.MAIN_END)

    if FORECAST_ISSUE_DATE:
        # Log out predictions in Forecast Stage mode
        pd.set_option("display.max_columns", None)
        pd.set_option("display.expand_frame_repr", False)
        logger.info("Generated predictions:\n{}", submission_format_df)


if __name__ == "__main__":
    main()
