{% test date_range_tolerance(model, column_name, range_type, after, before, tolerance) %}
    {% set whereClause = "" %}
    
    {% if range_type == 'Before' %}
        {% set whereClause = column_name + ' >= TIMESTAMP ' + "'" + before + "'" %}
    {% elif range_type == 'After' %}
        {% set whereClause = column_name + ' < TIMESTAMP ' + "'" + after + "'" %}        
    {% else %}
        {% set whereClause = column_name + ' NOT BETWEEN TIMESTAMP ' +  "'" + after + "'" + ' AND TIMESTAMP ' + "'" + before + "'" %}
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
    WHERE ((select COUNT(*) from OutOfRangeValues) * 100.0) / (SELECT * FROM NumberOfRecords) >= {{ tolerance }}

{% endtest %}   