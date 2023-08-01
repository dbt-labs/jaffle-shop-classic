{{
    config(
        materialized='incremental',
        unique_key='customer_name',
        incremental_strategy='merge'
    )
}}

WITH

using_clause AS (

    SELECT
        customer_id,
        customer_name,
        MAX(dtloaded) as dtloaded

    FROM {{ ref('stg_customers') }}


    {% if is_incremental() %}

        WHERE dtloaded > (SELECT MAX(dtloaded) FROM {{ this }})

    {% endif %}

    GROUP BY customer_name, customer_id

),

updates AS (

    SELECT
        customer_id,
        customer_name,
        now() as dtupdated,
        NULL as dtinserted,
        dtloaded

    FROM using_clause

    {% if is_incremental() %}

        WHERE customer_name IN (SELECT customer_name FROM {{ this }})

    {% endif %}

),

inserts AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_name'])}} as customer_id,
        customer_name,
        NULL as dtupdated,
        now() as dtinserted,
        dtloaded
    FROM using_clause
    WHERE customer_name NOT IN (SELECT customer_name FROM updates)
 )

SELECT customer_id,customer_name,dtloaded,dtupdated,cast(dtinserted as timestamp)
FROM updates
UNION
SELECT customer_id,customer_name,dtloaded,cast(dtupdated as timestamp),dtinserted
FROM inserts