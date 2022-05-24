view: orders {
  sql_table_name: `bicycle-health-dbt-dev`.`dbt_geoff_jaffle_shop`.`orders` ;;

  dimension_group: order_date {
    type: time
    sql: ${TABLE}.order_date ;;
    description: "Date (UTC) that the order was placed"
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

  dimension: order_id {
    type: number
    sql: ${TABLE}.order_id ;;
    description: "This is a unique identifier for an order"
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
    description: "Foreign key to the customers table"
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
    description: "Orders can be one of the following statuses:

| status         | description                                                                                                            |
|----------------|------------------------------------------------------------------------------------------------------------------------|
| placed         | The order has been placed but has not yet left the warehouse                                                           |
| shipped        | The order has ben shipped to the customer and is currently in transit                                                  |
| completed      | The order has been received by the customer                                                                            |
| return_pending | The customer has indicated that they would like to return the order, but it has not yet been received at the warehouse |
| returned       | The order has been returned by the customer and received at the warehouse                                              |"
  }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
    description: "Total amount (AUD) of the order"
  }

  dimension: credit_card_amount {
    type: number
    sql: ${TABLE}.credit_card_amount ;;
    description: "Amount of the order (AUD) paid for by credit card"
  }

  dimension: coupon_amount {
    type: number
    sql: ${TABLE}.coupon_amount ;;
    description: "Amount of the order (AUD) paid for by coupon"
  }

  dimension: bank_transfer_amount {
    type: number
    sql: ${TABLE}.bank_transfer_amount ;;
    description: "Amount of the order (AUD) paid for by bank transfer"
  }

  dimension: gift_card_amount {
    type: number
    sql: ${TABLE}.gift_card_amount ;;
    description: "Amount of the order (AUD) paid for by gift card"
  }
}