select ordernumber, quantityordered, priceeach, orderlinenumber, sales, orderdate, status
, qtr_id, month_id, year_id, productline, msrp, productcode, customername
, phone, addressline1, addressline2, city, state, postalcode, country
, territory, contactlastname, contactfirstname, dealsize, organisation, day_id
, customerid
, now() as dtloaded
 from {{ source('public', 'stgsales') }}