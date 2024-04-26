{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle_and_transactionlog CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle_and_transactionlog AS
    SELECT
        z.*,
        t.*
    FROM {{ schema }}.clean__zettle__transaction__summary z
    FULL OUTER JOIN {{ schema }}.clean__transaction__summary t ON t.payment_reference_id = z.zettle_purchase_id AND z.zettle_originatortransactiontype='PAYMENT'
    WHERE (z.zettle_purchase_created_dim_date >= '2023-08-01' OR t.payment_at_dim_date >= '2023-08-01' )
    AND z.zettle_originatortransactiontype='PAYMENT'

WITH NO DATA;
{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS transaction__item__summary_idx ON {{ schema }}.rep__transaction__item__summary (transaction_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle_and_transactionlog;
