WITH order_attributes AS (

    SELECT *
    FROM {{ ref('stg_order_attributes') }}

), orders AS (

    SELECT *
    FROM {{ ref('orders') }}

), final AS (

    SELECT
        *,

        -- the following column is filled in a fal after script
        NULL AS cluster_label

    FROM order_attributes

    LEFT JOIN orders
        USING (order_id)

)

SELECT * FROM final

