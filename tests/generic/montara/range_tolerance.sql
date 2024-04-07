{% test range_tolerance(model, column_name, min, max, tolerance) %}
    
    WITH NumberOfRecords AS (
        SELECT 
        COUNT(*) 
        FROM {{ model }}
    ), 
    OutOfRangeValues AS (
        SELECT *
        FROM {{ model }}
        WHERE {{ column_name }} NOT BETWEEN {{ min }} AND {{ max }}
    )
    SELECT * 
    FROM OutOfRangeValues
    WHERE (((select COUNT(*) from OutOfRangeValues) :: FLOAT / (SELECT * FROM NumberOfRecords) :: FLOAT) * 100) >= {{ tolerance }}

{% endtest %}