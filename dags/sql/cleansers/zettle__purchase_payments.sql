{% if is_modified %}

CREATE OR REPLACE VIEW {{ schema }}.clean__zettle__purchase_payments AS
SELECT
    p.id AS purchase_id,
    p.created,
    p.airflow_sync_ds,
    x.*
FROM
    raw__zettle__purchases p
    CROSS JOIN LATERAL (
        SELECT
            p.id || '__' || (value ->> 'uuid')::text AS id,
            (value ->> 'uuid')::text AS uuid,
            (value ->> 'amount')::bigint AS amount,
            (value ->> 'type')::text AS "type",
            (value ->> 'gratuityAmount')::bigint AS gratuityAmount
        FROM
            json_array_elements(CAST(p.payments AS JSON))
    ) AS x;
{% endif %}
