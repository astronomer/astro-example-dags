{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__shopify_partner_monthly_summary CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__shopify_partner_monthly_summary AS
    WITH
        orders AS
        (SELECT
            po.*,
            CASE
                WHEN tags LIKE '%%harper%%' OR payment_gateway_names LIKE '%%Harper Payments%%' THEN 'Harper'
                WHEN source_name = 'web' THEN 'Web'
            END AS source,
            CASE
                WHEN harper_product = 'harper_try' THEN 'Harper Try'
                WHEN (tags LIKE '%%harper%%' OR payment_gateway_names LIKE '%%Harper Payments%%' ) AND harper_product IS NULL THEN 'Harper Concierge'
                ELSE NULL
            END AS harper_product_
        FROM
            {{ schema }}.shopify_partner_orders po
        )

    SELECT
        TO_CHAR(created_at, 'YYYY-MM') AS month,
        partner__name,
        source,
        harper_product_,
        COUNT(DISTINCT name) AS orders,
        SUM(items_ordered) AS total_items_ordered,
        SUM(items_returned) AS total_items_returned,
        SUM(items_ordered) - SUM(items_returned) AS total_items_kept,
        ROUND(SUM(value_ordered)::numeric, 2) AS total_value_ordered,
        ROUND(SUM(value_returned)::numeric, 2) AS total_value_returned,
        ROUND((SUM(value_ordered) - SUM(value_returned))::numeric, 2) AS total_value_kept
    FROM
        orders
    WHERE cancelled_at IS NULL
        AND cancel_reason IS NULL
    GROUP BY
        partner__name,
        TO_CHAR(created_at, 'YYYY-MM'),
        source,harper_product_
    ;

WITH NO DATA;

{% if is_modified %}
CREATE INDEX IF NOT EXISTS rep__shopify_partner_monthly_summary_month_idx ON {{ schema }}.rep__shopify_partner_monthly_summary(month);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__shopify_partner_monthly_summary;
