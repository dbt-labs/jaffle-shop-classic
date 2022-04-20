"""Forecast and upload order data
Packages:
 - prophet
"""

import pandas as pd
from prophet import Prophet
import sqlalchemy.types as types


def make_forecast(dataframe: pd.DataFrame, col: str, periods: int = 30, plot = False):
    """Make forecast on metric data."""
    dataframe = dataframe[["order_date", col]]
    dataframe = dataframe.rename(columns={"order_date": "ds", col: "y"})

    model = Prophet(daily_seasonality=False, yearly_seasonality=False)
    model.fit(dataframe)

    future = model.make_future_dataframe(periods=periods)
    prediction = model.predict(future)

    if plot:
        plot_forecast(model, prediction, col)

    return prediction


def plot_forecast(model: Prophet, forecast: pd.DataFrame, filename: str):
    from prophet.plot import plot_plotly

    fig = plot_plotly(model, forecast)
    fig.write_image(f"{context.current_model.name}_{filename}.jpg")


df: pd.DataFrame = ref("orders_daily")
print(df)

forecast_count = make_forecast(df, "order_count", 50)
forecast_amount = make_forecast(df, "order_amount", 50)

joined_forecast = forecast_count.join(
    forecast_amount.set_index("ds"),
    on="ds",
    rsuffix="_amount",
)

for cluster in [0, 1, 2]:
    cluster_col = f"cluster_{cluster}"
    forecast_cluster = make_forecast(df, cluster_col, 50)

    joined_forecast = joined_forecast.join(
        forecast_cluster.set_index("ds"),
        on="ds",
        rsuffix=f"_{cluster_col}",
    )

with pd.option_context('display.max_rows', None):
    # Show all dtypes
    print(joined_forecast.dtypes)

# HACK: have to figure out how to write dates (or datetimes) to the database
# TODO: The types.DATE did not work when testing for `dtype={"ds": types.DATE}`
joined_forecast["ds"] = joined_forecast["ds"].map(lambda x: x.strftime("%Y-%m-%d"))

# Generates a table with a BUNCH of columns
# It will use the current model as target, no need to pass it
write_to_model(joined_forecast, mode="overwrite")
