DROP VIEW IF EXISTS {{ schema }}.rep__ship_direct_orders CASCADE;
CREATE VIEW {{ schema }}.rep__ship_direct_orders AS
SELECT b.order_name AS original_order_name, a.*
    FROM (
        SELECT *
        FROM {{ schema }}.rep__order__summary.sql
        WHERE
        order_type = 'ship_direct') a
    LEFT JOIN public.clean__order__summary b
    ON a.initiated_sale__original_order_id = b.id
    WHERE a.order_status != 'cancelled'
    AND b.link_order__is_child != 1;
