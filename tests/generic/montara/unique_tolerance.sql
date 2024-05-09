{% test unique_tolerance(model, column_name, tolerance) %}
	WITH 
	NumberOfRecords AS (
        SELECT 
        COUNT(*) 
        FROM {{ model }}
    ), 
	NonUniqueValues as (
        SELECT 
            {{ column_name }} 
        FROM 
            {{ model }}
        GROUP BY 
            {{ column_name }}
        HAVING 
            COUNT({{ column_name }}) > 1
		)
    SELECT * FROM {{ model }} m 
	join NonUniqueValues on NonUniqueValues.{{ column_name }} = m.{{ column_name }}   
	WHERE  ((select COUNT(*) from NonUniqueValues) * 100.0) / (SELECT * FROM NumberOfRecords) >= 0	    

{% endtest %}