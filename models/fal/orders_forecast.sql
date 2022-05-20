
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 1870f46fbe5fb3015215099696dcf81b

Script dependencies:

{{ ref('orders_daily') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.name }}
