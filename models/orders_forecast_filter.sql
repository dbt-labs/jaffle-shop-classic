WITH orders_forecast AS (
    
    SELECT * FROM {{ ref('orders_forecast') }}
    
), final AS (
    
    SELECT
        ds AS order_date,
        yhat AS forecast_count,
        yhat_amount AS forecast_amount,
        yhat_cluster_0 AS forecast_cluster_0,
        yhat_cluster_1 AS forecast_cluster_1,
        yhat_cluster_2 AS forecast_cluster_2
    FROM orders_forecast
    
)

SELECT * FROM final
