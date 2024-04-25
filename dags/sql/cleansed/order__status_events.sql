{% macro generate_sql_parts(event_names) %}
    {% for event_name in event_names %}
        {% if not loop.first %}, {% endif %}
        MAX(CASE WHEN event_name_id = '{{ event_name }}' THEN createdat ELSE NULL END) AS "{{ event_name }}_at"
    {% endfor %}
{% endmacro %}

{% if is_modified %}
    DROP VIEW IF EXISTS {{ schema }}.order__status_events CASCADE;
{% endif %}
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = '{{ schema }}'
        AND    c.relname = 'order__status_events'
        AND    c.relkind = 'v' -- 'v' stands for view
    ) THEN
        EXECUTE '

CREATE OR REPLACE VIEW public.order__status_events AS
SELECT order_id,
    {{ generate_sql_parts(event_name_ids) }}
FROM orderevents
GROUP BY order_id;

';
    END IF;
END
$$;
