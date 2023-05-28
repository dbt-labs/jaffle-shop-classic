--AutomateDV code to generate a staging table in DV model

{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "stg_orders"
derived_columns:
  SOURCE: "!1"
  LOAD_DATETIME: "28/05/2023"
  EFFECTIVE_FROM: "ORDER_DATE"
  START_DATE: "ORDER_DATE"
  END_DATE: "TO_DATE('99991231','YYYYMMDD')"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict["source_model"],
                     derived_columns=metadata_dict["derived_columns"],
                     null_columns=null_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=ranked_columns) }}


