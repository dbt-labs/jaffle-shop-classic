{% test accepted_values_tolerance(model, column_name , tolerance, value) %}
    
    WITH AcceptedValuesCounts AS (
        SELECT *
        FROM {{ model }}
        where {{ column_name }} not in ('{{ value | join("', '") }}')
    ),
    AllValuesCounts AS (
        SELECT count(*)
        FROM {{ model }}
    )
    SELECT * FROM AcceptedValuesCounts
    WHERE ((select COUNT(*) from AcceptedValuesCounts) :: FLOAT / (select * from AllValuesCounts) :: FLOAT) * 100 >= {{ tolerance }}

{% endtest %}