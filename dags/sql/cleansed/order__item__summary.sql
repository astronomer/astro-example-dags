{% if is_modified %}
DROP VIEW IF EXISTS {{ schema }}.clean__order__item__summary CASCADE;
{% endif %}

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = '{{ schema }}'
        AND    c.relname = 'order__status_events'
        AND    c.relkind = 'v' -- 'v' stands for view
    ) THEN
        EXECUTE '

CREATE OR REPLACE VIEW {{ schema }}.clean__order__item__summary AS
    SELECT
        o.id AS order_id,
        COUNT(oi.id) AS total_items,
        COUNT(CASE WHEN oi.fulfilled = TRUE THEN 1 END) AS num_items_fulfilled,
        COUNT(CASE WHEN oi.purchased = TRUE THEN 1 END) AS num_purchased,
        COUNT(CASE WHEN oi.returned = TRUE THEN 1 END) AS num_returned,
        COUNT(CASE WHEN oi.purchased = TRUE AND oi.returned = FALSE THEN 1 END) AS num_kept,
        COUNT(CASE WHEN oi.preorder = TRUE THEN 1 END) AS num_preorder,
        COUNT(CASE WHEN oi.received = TRUE THEN 1 END) AS num_received_by_harper_warehouse,
        COUNT(CASE WHEN oi.received_by_warehouse = TRUE THEN 1 END) AS num_received_by_partner_warehouse,
        COUNT(CASE WHEN oi.return_requested_by_customer = TRUE THEN 1 END) AS num_return_requested_by_customer,
        COUNT(CASE WHEN oi.return_sent_by_customer = TRUE THEN 1 END) AS num_return_sent_by_customer,
        array_agg(DISTINCT(oi.tracking_url)) AS delivery_tracking_urls

    FROM
        {{ schema }}.orders o
    JOIN
        order__items oi ON o.id = oi.order_id
    GROUP BY
        o.id;

';
    END IF;
END
$$;
