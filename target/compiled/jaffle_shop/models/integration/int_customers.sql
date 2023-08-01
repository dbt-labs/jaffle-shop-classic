

WITH

using_clause AS (

    SELECT
        customer_id,
        customer_name,
        MAX(dtloaded) as dtloaded

    FROM "dbtsales"."public_stg"."stg_customers"


    

        WHERE dtloaded > (SELECT MAX(dtloaded) FROM "dbtsales"."public"."int_customers")

    

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

    

        WHERE customer_name IN (SELECT customer_name FROM "dbtsales"."public"."int_customers")

    

),

inserts AS (

    SELECT
        md5(cast(coalesce(cast(customer_name as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customer_id,
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