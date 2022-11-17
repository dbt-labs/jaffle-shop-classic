    select 
        date_day,
        customer_id,
        order_count
    from {{metrics.calculate(
        metric('order_count'),
        grain='day',
        dimensions=['customer_id'])}}
        /*,
        start_date=var('load_date')*/