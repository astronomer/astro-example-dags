{% macro generate_sql_parts(event_names) %}
    {% for event_name in event_names %}
        {% if not loop.first %}, {% endif %}
        MAX(CASE WHEN event_name_id = '{{ event_name }}' THEN createdat ELSE NULL END) AS "{{ event_name }}_at"
    {% endfor %}
{% endmacro %}

{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.order__status_events AS
SELECT order_id,
    {{ generate_sql_parts(event_name_ids) }}
FROM orderevents
GROUP BY order_id;
{% endif %}
