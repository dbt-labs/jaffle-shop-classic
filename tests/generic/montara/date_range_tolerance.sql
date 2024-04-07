{% test date_range_tolerance(model, column_name, range_type, after, before, tolerance) %}
    {% set whereClause = "" %}
    
    {% if range_type == 'Before' %}
        {% set whereClause = column_name + ' >= ' + "'" + before + "'" %}
    {% elif range_type == 'After' %}
        {% set whereClause = column_name + ' < ' + "'" + after + "'" %}        
    {% else %}
        {% set whereClause = column_name + ' NOT BETWEEN ' +  "'" + after + "'" +' AND ' + "'" + before + "'" %}
    {% endif %}

    WITH NumberOfRecords AS (
        SELECT 
        COUNT(*) 
        FROM {{ model }}
    ), 
    
    OutOfRangeValues AS (
        SELECT *
        FROM {{ model }}
        WHERE {{ whereClause }}
    )
    SELECT * 
    FROM OutOfRangeValues
    WHERE (((select COUNT(*) from OutOfRangeValues) :: FLOAT / (SELECT * FROM NumberOfRecords) :: FLOAT) * 100) >= {{ tolerance }}

{% endtest %}   