#!/bin/bash

# this script captures the delta between the stock jaffle_shop project contents and the enhancements we need to support testing of our collectors
# run this script to create a commit in the fork

# add a snapshot config
mkdir -p snapshots && cat > snapshots/orders.sql << EOF
{% snapshot orders_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
      meta={'snapshot_meta_key': 'snapshot meta test value'}
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}
EOF

# add a sources config
cat > models/sources.yml << EOF
version: 2

sources:
  - name: jaffle_shop
    description: a demo source for jaffle shop
    schema: KOS_DBT_TEST_MANUAL
    meta:
      favorite_color: red
    tables:
      - name: stg_orders
        meta:
          is_source_table: true
        columns:
          - name: id
            tests:
              - unique
              - not_null
          - name: order_id
            meta:
              column_note: "column meta test value"
EOF

# use source reference rather than table name for stg_orders table
sed -i '' "s/ref('stg_orders')/source('jaffle_shop', 'stg_orders')/g" ./models/orders.sql

# add a timestamp column to orders so it can be snapshotted
sed -i '' "s/total_amount as amount/total_amount as amount, current_timestamp as updated_at/g" ./models/orders.sql

# add meta values to the dbt_project.yml file
cat >> ./dbt_project.yml << EOF
  +meta:
    all-models: "all models meta"

seeds:
  +meta:
    all-seeds: "all seeds meta"

tests:
  +meta:
    all-tests: "all tests meta"
EOF

# add meta values to the schema.yml file
sed -i '' 's/- name: orders/- name: orders\n    config:\n      meta:\n        orders_meta: "model meta test for orders"/' ./models/schema.yml

# add meta values to the customers.sql model file
sed -i '' "1s/^/{{ config(meta = {'customers_sql': 'applies to one model in sql config'}) }}\n/" ./models/customers.sql
