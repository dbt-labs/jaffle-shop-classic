view: customers {
  sql_table_name: `bicycle-health-dbt-dev`.`dbt_geoff_jaffle_shop`.`customers` ;;

  dimension_group: first_order {
    type: time
    sql: ${TABLE}.first_order ;;
    description: "Date (UTC) of a customer's first order"
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension_group: most_recent_order {
    type: time
    sql: ${TABLE}.most_recent_order ;;
    description: "Date (UTC) of a customer's most recent order"
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: "This is a unique identifier for a customer"
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
    description: "Customer's first name. PII."
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
    description: "Customer's last name. PII."
  }

  dimension: number_of_orders {
    type: number
    sql: ${TABLE}.number_of_orders ;;
    description: "Count of the number of orders a customer has placed"
  }
}