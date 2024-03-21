{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__transaction__summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__transaction__summary AS
    SELECT
    t.*,
    tis.*,
    dt.*
FROM
    {{ schema }}.transaction t
LEFT JOIN {{ schema }}.rep__transaction__item__summary tis ON tis.transaction_id=t.id
LEFT JOIN {{ schema }}.dim__time dt ON t.payment_at::date = dt.dim_date_id

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS transaction__summary_idx ON {{ schema }}.rep__transaction__summary (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__transaction__summary;
