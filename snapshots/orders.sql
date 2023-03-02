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
