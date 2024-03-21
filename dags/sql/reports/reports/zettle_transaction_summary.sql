{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle__transaction_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle__transaction_summary AS
select
    zt.amount as transaction_amount,
    zt.originatingtransactionuuid as transaction_originatingtransactionuuid,
    zt.originatortransactiontype as transaction_originatortransactiontype,
    zt.timestamp as transaction_timestamp,
    zt.airflow_sync_ds as transaction_airflow_sync_ds,
    zpp.*
FROM {{ schema }}.zettle__transactions zt
LEFT JOIN {{ schema }}.rep__zettle__purchase_payments zpp ON zt.originatingtransactionuuid=zpp.uuid
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS zettle__transaction_summary_idx ON {{ schema }}.rep__zettle__transaction_summary (id, transaction_originatortransactiontype);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle__transaction_summary;
