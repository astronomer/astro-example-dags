{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__order_reference CASCADE;
{% endif %}
-- This report shows the links between harper orders and partner order names
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__order_reference AS
WITH orders AS (
    SELECT
        brand_name,
        -- id,
        createdat__dim_date,
        order_name,
        original_order_name
    FROM clean__order__summary
    WHERE link_order__is_child = 0
),

linked AS (
    SELECT
        order_name,
        '[' || STRING_AGG(DISTINCT original_order_name::text, ' , ' ORDER BY original_order_name ASC) || ']' AS partner_order_name
    FROM public.clean__order__items oi
    GROUP BY order_name
),

combined AS (
    SELECT
        o.brand_name,
        -- o.id,
        o.createdat__dim_date,
        o.original_order_name,
        o.order_name,
        l.partner_order_name
    FROM linked l
    LEFT JOIN orders o
        ON o.order_name = l.order_name
    WHERE o.order_name IS NOT NULL
    ORDER BY o.original_order_name
)

SELECT
    brand_name,
    -- id,
    MIN(createdat__dim_date) AS min_created_at,
    original_order_name,
    STRING_AGG(order_name, ', ' ORDER BY order_name ASC) AS harper_order_name,
    STRING_AGG(partner_order_name, ', ' ORDER BY partner_order_name ASC) AS partner_order_name
FROM combined
GROUP BY original_order_name, brand_name
ORDER BY original_order_name;

{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS rep__order_reference_idx ON {{ schema }}.rep__order_reference (id);
CREATE INDEX IF NOT EXISTS rep__order_reference_harper_order_name ON {{ schema }}.rep__order_reference (harper_order_name);
CREATE INDEX IF NOT EXISTS rep__order_reference_partner_order_name ON {{ schema }}.rep__order_reference (partner_order_name);
CREATE INDEX IF NOT EXISTS rep__order_reference_original_order_name ON {{ schema }}.rep__order_reference (original_order_name);
{% endif %}
