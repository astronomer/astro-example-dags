DROP VIEW IF EXISTS {{ schema }}.clean__order__item__summary CASCADE;
CREATE VIEW {{ schema }}.clean__order__item__summary AS
    SELECT
        o.id AS order_id,
        COUNT(oi.id) AS total_items,
        SUM(oi.fulfilled) AS num_items_fulfilled,
        SUM(oi.purchased) AS num_purchased,
        SUM(oi.returned) AS num_returned,
        SUM(oi.purchased) AS num_actually_purchased,
        SUM(oi.preorder) AS num_preorder,
        SUM(oi.received) AS num_received_by_harper_warehouse,
        SUM(oi.received_by_warehouse) AS num_received_by_partner_warehouse,
        SUM(oi.return_requested_by_customer) AS num_return_requested_by_customer,
        SUM(oi.return_sent_by_customer) AS num_return_sent_by_customer,
        array_agg(DISTINCT(oi.tracking_url)) AS delivery_tracking_urls
    FROM
        {{ schema }}.orders o
    JOIN
        order__items oi ON o.id = oi.order_id
    GROUP BY
        o.id;
