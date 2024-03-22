{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__unreconciled_transactions CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__unreconciled_transactions AS
    select
        u.zettle_transaction_amount AS zettle_txn_amount,
        u.zettle_purchase_amount AS zettle_payment_purchase_amount,
        u.payment_invoiced_amount AS translog_zettle_invoiced_amount,
        u.total_amount AS translog_calculated_total_amount,
        u.payment_at AS translog_payment_at,
        u.zettle_created AS zettle_payment_at,
        u.*

    FROM {{ schema }}.rep__zettle_and_transactionlog u
    WHERE (u.payment_reference_id IS NULL OR u.zettle_purchase_id IS NULL OR u.total_amount <> u.zettle_purchase_amount)

WITH NO DATA;
{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS transaction__item__summary_idx ON {{ schema }}.rep__transaction__item__summary (transaction_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__unreconciled_transactions;
