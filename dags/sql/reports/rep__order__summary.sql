{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__order__summary CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__order__summary AS
    SELECT
        *
    FROM clean__order__summary o
    ORDER BY createdAt ASC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__order__summary_idx ON {{ schema }}.rep__order__summary (id);
CREATE INDEX IF NOT EXISTS rep__order__summary_order_name_idx ON {{ schema }}.rep__order__summary (order_name);
CREATE INDEX IF NOT EXISTS rep__order__summary_original_order_name_idx ON {{ schema }}.rep__order__summary (original_order_name);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__order__summary;
