{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__zettle__purchase_products CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__zettle__purchase_products AS
SELECT
    p.id AS purchase_id,
    p.created,
    p.airflow_sync_ds,
    x.*
FROM
    raw__zettle__purchases p
    CROSS JOIN LATERAL (
        SELECT
            p.id || '__' || (value ->> 'id')::text || '__' || (value ->> 'productUuid')::text || '__' || (value ->> 'variantUuid')::text AS id,
            -- (value ->> 'id')::text AS id,
            (value ->> 'name')::text AS name,
            (value ->> 'variantName')::text AS variantName,
            (value ->> 'type')::text AS "type",
            (value ->> 'quantity')::text AS quantity,
            (value ->> 'productUuid')::text AS zettle_uuid,
            (value ->> 'variantUuid')::text AS zettle_variantuuid,
            (value ->> 'libraryProduct')::boolean AS libraryproduct,
            (value ->> 'unitPrice')::numeric AS unitPrice,
            (value ->> 'grossValue')::numeric AS grossValue
        FROM
            json_array_elements(CAST(p.products AS JSON))
    ) AS x
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS zettle__purchase_products_idx ON {{ schema }}.rep__zettle__purchase_products (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__zettle__purchase_products;
