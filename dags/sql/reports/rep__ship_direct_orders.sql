DROP VIEW IF EXISTS {{ schema }}.rep__ship_direct_orders CASCADE;
CREATE VIEW {{ schema }}.rep__ship_direct_orders AS
SELECT
    b.order_name AS previous_order_name,
    b.original_order_name AS previous_original_order_name,
    b.id AS previous_order_id,
    a.*
    FROM (
        SELECT *
        FROM {{ schema }}.clean__order__summary
        WHERE
        order_type = 'ship_direct') a
    LEFT JOIN {{ schema }}.clean__order__summary b
    ON a.initiated_sale__original_order_id = b.id
    WHERE a.order_status != 'cancelled'
    AND a.order_cancelled_status NOT IN ('Cancelled post shipment','Cancelled - no email triggered','Cancelled pre shipment')
    AND b.link_order__is_child != 1;
