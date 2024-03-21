{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__transaction__item__summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__transaction__item__summary AS
    SELECT
    transaction_id,
    count(*) as item_count,
    SUM(CASE
        WHEN transaction_type IN ('discount', 'refund') THEN -lineitem_amount
        ELSE lineitem_amount
    END) AS total_amount
FROM
    {{ schema }}.transaction__items
GROUP BY
    transaction_id
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS transaction__item__summary_idx ON {{ schema }}.rep__transaction__item__summary (transaction_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__transaction__item__summary;
