{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__stripe_transactions CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__stripe_transactions AS
    SELECT
        s.id AS charge_id,
        s.invoice AS invoice_id
        adt.*,
    FROM
        {{ schema }}.stripe__transactions s
    LEFT JOIN
        dim__time adt ON TO_CHAR(TO_TIMESTAMP(s.created), 'YYYY-MM-DD') = adt.id

WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__stripe_trans_chg_idx ON {{ schema }}.rep__stripe_transactions (charge_id);
CREATE INDEX IF NOT EXISTS rep__stripe_trans_inv_idx ON {{ schema }}.rep__stripe_transactions (invoice_id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__stripe_transactions;
