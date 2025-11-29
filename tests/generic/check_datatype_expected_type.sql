{% test check_datatype_expected_type(model, column_name, expected_type) %}

    WITH validation AS (
        SELECT
            {{ column_name }} AS column_value
        FROM
            {{ model }}
    )

    SELECT
        column_value
    FROM
        validation
    WHERE
        {% if expected_type == 'numeric' %}
        SAFE_CAST(column_value AS NUMERIC) IS NULL
        {% elif expected_type == 'date' %}
        SAFE_CAST(column_value AS DATE) IS NULL
        {% elif expected_type == 'timestamp' %}
        SAFE_CAST(column_value AS TIMESTAMP) IS NULL
        {% else %}
    
        'Error: Unsupported expected_type provided to check_datatype_expected_type test' IS NULL
        {% endif %}
        AND column_value IS NOT NULL

{% endtest %}
