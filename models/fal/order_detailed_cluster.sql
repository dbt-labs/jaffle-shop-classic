
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED c8010249dc44baee07f7f4acb5e0ba40

Script dependencies:

{{ ref('order_detailed') }}

*/

SELECT * FROM {{ target.schema }}.{{ model.alias }}
