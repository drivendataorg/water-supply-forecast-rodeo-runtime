"""This is a template for the expected code submission format. Your solution must
implement the 'predict' function. The 'preprocess' function is optional."""

from collections.abc import Hashable
from pathlib import Path
from typing import Any


def preprocess(src_dir: Path, data_dir: Path, preprocessed_dir: Path) -> dict[Hashable, Any]:
    """An optional function that performs setup or processing. You can do
    whatever you want in this function. Some examples may include:

    - Downloading additional data that you need for data sources approved for
      direct API access
    - Preprocess feature data and writing intermediate outputs to the
      preprocessed directory
    - Loading assets, such as model weights, that you intend to use across
      predictions

    If you need to save any intermediate outputs, you should write them to the
    provided `preprocessed_dir` path. This is a directory that you will have
    write permissions to use.

    This function, if it is defined, will be run before your `predict` function
    is called.

    Args:
        src_dir (Path): path to the directory that your submission ZIP archive
            contents are unzipped to.
        data_dir (Path): path to the mounted data drive.
        preprocessed_dir (Path): path to a directory where you can save any
            intermediate outputs for later use.

    Returns:
        (dict[Hashable, Any]): a dictionary containing any assets you want to
            hold in memory that will be passed to to your 'predict' function as
            the keyword argument 'assets'.
    """
    return {}


def predict(
    site_id: str,
    issue_date: str,
    assets: dict[Hashable, Any],
    src_dir: Path,
    data_dir: Path,
    preprocessed_dir: Path,
) -> tuple[float, float, float]:
    """A function that generates a forecast for a single site on a single issue
    date. This function will be called for each site and each issue date in the
    test set.

    Args:
        site_id (str): the ID of the site being forecasted.
        issue_date (str): the issue date of the site being forecasted in
            'YYYY-MM-DD' format.
        assets (dict[Hashable, Any]): a dictionary of any assets that you may
            have loaded in the 'preprocess' function.
        src_dir (Path): path to the directory that your submission ZIP archive
            contents are unzipped to.
        data_dir (Path): path to the mounted data drive.
        preprocessed_dir (Path): path to a directory where you can save any
            intermediate outputs for later use.
    Returns:
        tuple[float, float, float]: forecasted values for the seasonal water
        supply. The three values should be (0.10 quantile, 0.50 quantile,
        0.90 quantile).
    """
    return 0.0, 0.0, 0.0
