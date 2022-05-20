
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED 4c5fecf57653e134322b04d630e9f935

Script dependencies:

{{ ref('order_detailed') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.name }}
