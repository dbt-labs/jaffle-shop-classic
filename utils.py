"""Forecast and upload order data
Packages:
 - prophet
"""

import pandas as pd
from prophet import Prophet


def make_forecast(dataframe: pd.DataFrame, periods: int = 30, plot=False):
    """Make forecast on metric data."""
    dataframe = dataframe[["ds", "y"]]

    model = Prophet(daily_seasonality=False, yearly_seasonality=False)
    model.fit(dataframe)

    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    if plot:
        _plot_forecast(model, forecast)

    return forecast


def _plot_forecast(model: Prophet, forecast: pd.DataFrame):
    from prophet.plot import plot_plotly

    from datetime import datetime

    suffix = datetime.now().strftime("%s")

    fig = plot_plotly(model, forecast)
    fig.write_image(f"forecast_{suffix}.jpg")
