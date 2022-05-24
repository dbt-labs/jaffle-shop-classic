view: stg_orders {
  sql_table_name: `bicycle-health-dbt-dev`.`dbt_geoff_jaffle_shop`.`stg_orders` ;;

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
    description: ""
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: ""
  }
}