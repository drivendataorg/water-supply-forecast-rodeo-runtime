# Cross-validation example

This is a example of performing a year-wise leave-one-out cross-validation for the [Final Prize Stage](https://www.drivendata.org/competitions/262/reclamation-water-supply-forecast-final/page/871/). It uses an unrealisticly simple model for demonstration purposes.

## Dependencies

Create a virtual environment and run:

```bash
pip install requirements.txt
```

## Data

Download the following files from the [data download page](https://www.drivendata.org/competitions/262/reclamation-water-supply-forecast-final/data/) and put them in the `data/` directory in the root of this repository (one directory level up from this directory).

- `cross_validation_labels.csv` -> `../data/cross_validation_labels.csv`
- `cross_validation_submission_format.csv` -> `../data/cross_validation_submission_format.csv`
- `cross_validation_monthly_flow.csv` -> `../data/cross_validation_monthly_flow.csv`

## Run the script

```bash
python example.py
```

Cross-validation scores are written out to `cv_scores.json`. Predictions are written out to `cv_submission.csv` in the format that you would submit. Each iteration's models are saved to `models/`.
