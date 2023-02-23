{% snapshot orders_snapshot_timestamp %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='date_day',
          materialization="view"
        )
    }}

select 
  1 as id, 
  'blue' as color, 
  cast('2019-01-01' as date) as date_day

{% endsnapshot %}