{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__zettle__purchase_products AS
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
        ) AS x;
{% endif %}
