{% set order_statuses = ['returned', 'completed', 'return_pending', 'shipped', 'placed'] %}
{% set order_sizes = ['L', 'M', 'S'] %}
{% set order_clusters = [0, 1, 2] %}

WITH orders AS (

    SELECT * FROM {{ ref('order_detailed') }}

),

payments AS (

    SELECT * FROM {{ ref('stg_payments') }}

),

final AS (

    SELECT
        o.order_date,
        count(*) AS order_count,

        {% for status in order_statuses -%}
        sum(CASE WHEN o.status = '{{ status }}' THEN 1 ELSE 0 END) AS status_{{ status }},
        {% endfor %}

        {% for size in order_sizes -%}
        sum(CASE WHEN o.size = '{{ size }}' THEN 1 ELSE 0 END) AS size_{{ size }},
        {% endfor %}
        
        {% for cluster in order_clusters -%}
        sum(CASE WHEN o.cluster_label = {{ cluster }} THEN 1 ELSE 0 END) AS cluster_{{ cluster }},
        {% endfor %}

        sum(p.amount) AS order_amount
    FROM orders o

    LEFT JOIN payments p
        USING (order_id)

    GROUP BY o.order_date

)

SELECT * FROM final
