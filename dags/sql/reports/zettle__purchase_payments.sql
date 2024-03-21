{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle__purchase_payments CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle__purchase_payments AS
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
            (value ->> 'amount')::text AS amount,
            (value ->> 'type')::text AS "type",
            (value ->> 'gratuityAmount')::text AS gratuityAmount
        FROM
            json_array_elements(CAST(p.payments AS JSON))
    ) AS x
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS zettle__purchase_payments_idx ON {{ schema }}.rep__zettle__purchase_payments (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle__purchase_payments;
