view: stg_customers {
  sql_table_name: `bicycle-health-dbt-dev`.`dbt_geoff_jaffle_shop`.`stg_customers` ;;

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: ""
  }
}