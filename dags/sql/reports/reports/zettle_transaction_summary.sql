{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle__transaction_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle__transaction_summary AS
select
    zt.amount as zettle_amount,
    zt.originatingtransactionuuid as zettle_originatingtransactionuuid,
    zt.originatortransactiontype as zettle_originatortransactiontype,
    zt.timestamp as zettle_timestamp,
    zt.amount as zettle_transaction_amount,
    zt.airflow_sync_ds as zettle_transaction_airflow_sync_ds,
    zpp.purchase_id as zettle_purchase_id,
    zpp.created as zettle_created,
    zpp.airflow_sync_ds as zettle_purchase_airflow_sync_ds,
    zpp.uuid as zettle_purchase_uuid,
    zpp.amount as zettle_purchase_amount,
    zpp.type as zettle_purchase_type,
    zpp.gratuityamount as zettle_purchase_gratuityamount,
    {{ dim__time_columns | prefix_columns('dt', 'zettle_purchase_created') }}
FROM {{ schema }}.zettle__transactions zt
LEFT JOIN {{ schema }}.rep__zettle__purchase_payments zpp ON zt.originatingtransactionuuid=zpp.uuid
LEFT JOIN dim__time dt ON zpp.created::date = dt.dim_date_id
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS zettle__transaction_summary_idx ON {{ schema }}.rep__zettle__transaction_summary (zettle_originatingtransactionuuid, zettle_originatortransactiontype, zettle_purchase_uuid);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle__transaction_summary;
