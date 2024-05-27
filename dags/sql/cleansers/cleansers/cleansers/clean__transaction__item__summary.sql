{% if is_modified %}
DROP VIEW IF EXISTS {{ schema }}.clean__transaction__item__summary CASCADE;
CREATE VIEW {{ schema }}.clean__transaction__item__summary AS
    SELECT
    transaction_id,
    count(*) as item_count,
    SUM(CASE
        WHEN transaction_type IN ('discount', 'refund') THEN -lineitem_amount
        ELSE lineitem_amount
    END) AS total_amount
FROM
    {{ schema }}.clean__transaction__items
GROUP BY
    transaction_id
;

{% endif %}
