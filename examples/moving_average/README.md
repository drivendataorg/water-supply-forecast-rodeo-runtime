# Moving average example solution

This directory contains an example solution for the Water Supply Forecast Rodeo. Please note that this model is not a realistic solution to the problem. It's purpose is as demonstration code of a submission that can run successfully and generate valid predictions.

The example demonstrates a few things:

- Implementation of the optional `preprocess` function. In this example, we use it to load a model asset that is included with the submission: precalculated mean and standard deviations of the season water supply for each site from the training data. We use this when the monthly naturalized flow time series data is missing for a particular site and test set year.
- A `predict` function that produces a forecast for a given site and issue date. The model implements a made-up time series forecasting approach—forward filling the last monthly value and then applying an exponentially-weighted moving average. The seasonal water supply is then the sum of the values from the specified months. If monthly data is not available, it falls back to the training data means.
- Loading certain data, like the site metadata and monthly naturalized flow dataset, using `wsfr-read` functions.
