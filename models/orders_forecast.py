"""Forecast and upload order data
Packages:
 - prophet
"""

import pandas as pd
from prophet import Prophet


def make_forecast(dataframe: pd.DataFrame, periods: int = 30):
    """Make forecast on metric data."""
    dataframe = dataframe[["ds", "y"]]

    model = Prophet(daily_seasonality=False, yearly_seasonality=False)
    model.fit(dataframe)

    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)

    return forecast



df: pd.DataFrame = ref("orders_daily")
print(df)

forecast_count = make_forecast(
    df.rename(columns={"order_date": "ds", "order_count": "y"}), 50
)
forecast_amount = make_forecast(
    df.rename(columns={"order_date": "ds", "order_amount": "y"}), 50
)

joined_forecast = forecast_count.join(
    forecast_amount.set_index("ds"),
    on="ds",
    rsuffix="_amount",
)

for cluster in [0, 1, 2]:
    cluster_col = f"cluster_{cluster}"
    forecast_cluster = make_forecast(df.rename(columns={"order_date": "ds", cluster_col: "y"}), 50)

    joined_forecast = joined_forecast.join(
        forecast_cluster.set_index("ds"),
        on="ds",
        rsuffix=f"_{cluster_col}",
    )

with pd.option_context("display.max_rows", None):
    # Show all dtypes
    print(joined_forecast.dtypes)

joined_forecast["ds"] = joined_forecast["ds"].map(lambda x: x.strftime("%Y-%m-%d"))

# Generates a table with a BUNCH of columns
# It will use the current model as target, no need to pass it
write_to_model(joined_forecast, mode="overwrite")
