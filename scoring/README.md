# Water Supply Forecast Rodeo Scoring Script

This is a script provided for you to locally calculate the [Averaged Mean Quantile Loss](https://www.drivendata.org/competitions/259/reclamation-water-supply-forecast/page/827/#primary-metric-quantile-loss) and the [Interval Coverage](https://www.drivendata.org/competitions/259/reclamation-water-supply-forecast/page/827/#secondary-metric-interval-coverage) metrics on predictions for the [Water Supply Forecast Rodeo](https://watersupply.drivendata.org/) competition on DrivenData.

To use, first create a virtual environment and install the dependencies in `requirements.txt`:

```bash
pip install requirements.txt
```

The basic usage of this script is:

```bash
python score.py {path-to-ground-truth} {path-to-predictions}
```

where the ground truth file is a CSV file formatted like `train.csv` from the Hindcast Stage or `forecast_train.csv` labels files from the Forecast Stage ([documentation](https://www.drivendata.org/competitions/259/reclamation-water-supply-forecast/page/827/#labels-ground-truth-data)), and the predictions file is a CSV formatted like `submission_format.csv` ([documentation](https://www.drivendata.org/competitions/259/reclamation-water-supply-forecast/page/827/#example)). Note that the sites and years in the two files should exactly match.
