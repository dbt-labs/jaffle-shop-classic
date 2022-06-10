
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 30fca81df808442b361d9a42bb494aba

Script dependencies:

{{ ref('orders_daily') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
