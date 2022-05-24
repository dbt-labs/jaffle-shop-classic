view: stg_payments {
  sql_table_name: `bicycle-health-dbt-dev`.`dbt_geoff_jaffle_shop`.`stg_payments` ;;

  dimension: payment_id {
    type: number
    sql: ${TABLE}.payment_id ;;
    description: ""
  }

  dimension: payment_method {
    type: string
    sql: ${TABLE}.payment_method ;;
    description: ""
  }
}