{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__zettle__transaction__summary AS
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
    LEFT JOIN {{ schema }}.clean__zettle__purchase_payments zpp ON zt.originatingtransactionuuid=zpp.uuid
    LEFT JOIN dim__time dt ON zpp.created::date = dt.dim_date_id
;
{% endif %}
