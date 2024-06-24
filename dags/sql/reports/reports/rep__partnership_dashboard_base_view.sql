/*Partnership Dashboard Orders
1) CTE for first order with Harper for each customer
2) Item level report for the join of orders__items and orders
3) Two reports from (2): one grouped by order (linked to original order)
	and one grouped by item.order_type to analyze order makeup and split into parts (e.g., initiated sales)
To add: first time harper use with brand*/
{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__partnership_dashboard_base_view CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__partnership_dashboard_base_view AS
    WITH
    customer_first_order AS (
        SELECT
            customer_id,
            id,
            createdat,
            DATE(createdat) AS first_order_date
        FROM (
            SELECT
                customer_id,
                id,
                createdat,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY createdat) AS rn
            FROM
                {{ schema }}.clean__order__summary
        ) sub
        WHERE rn = 1
    ),
    order_items AS (
        SELECT
            CASE
                WHEN o.ship_direct = 1 THEN sd.previous_order_name
                ELSE o.order_name
            END AS order_name_merge, -- Parent order_name
            CASE
                WHEN o.ship_direct = 1 THEN sd.initiated_sale__original_order_id
                ELSE o.id
            END AS id_merge,
            o.*,
            i.*,
            o.order_type AS order__type,
            i.order_type AS item___order_type,
            o.order_name AS order__name,
            i.order_name AS item__order_name,
            i.createdat AS item__createdat,
            sd.createdat AS sd__createdat,
            o.createdat AS order__createdat,
            o.createdat__dim_date AS order__createdat__dim_date,
            i.is_initiated_sale AS item_is_initiated_sale,
            i.is_inspire_me AS item_is_inspire_me,
            sd.original_order_name AS original_id_ship_direct,
            i.item_value_pence AS item__item_value_pence,
            CASE
                WHEN fo.id = o.id THEN 'New Harper Customer'
                ELSE 'Returning Harper Customer'
            END AS customer_type_
        FROM
            {{ schema }}.rep__deduped_order_items i
        LEFT JOIN
            {{ schema }}.clean__order__summary o ON o.id = i.order_id
        LEFT JOIN
            customer_first_order fo ON fo.customer_id = o.customer_id
        LEFT JOIN
            {{ schema }}.rep__ship_direct_orders sd ON o.id = sd.id
        WHERE
            i.is_link_order_child_item = 0
            AND o.link_order__is_child = 0
    )
    SELECT
        MAX(appointment__date__dim_date) AS appointment__date__dim_date,
        MAX(appointment__date__dim_month) AS appointment__date__dim_month,
        MAX(appointment__date__dim_year) AS appointment__date__dim_year,
        brand_name,
        CASE
            WHEN MAX(CAST(item_is_initiated_sale AS INT)) = 1 THEN 1 ELSE 0
        END AS contains_initiated_sale,
        CASE
            WHEN MAX(CAST(item_is_inspire_me AS INT)) = 1 THEN 1 ELSE 0
        END AS contains_inspire_me,
        customer_id,
        customer_type_,
        discount_total,
        CASE
            WHEN order_status IN ('completed', 'returned', 'unpurchased_processed', 'return_prepared') THEN 'Happened'
            WHEN order_status = 'failed' THEN 'Failed'
            WHEN  (order_cancelled_status IN ('Cancelled post shipment','Cancelled - no email triggered','Cancelled pre shipment') OR order_status = 'Cancelled') THEN 'Cancelled'
            ELSE NULL
        END AS happened,
        id_merge,
        SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_purchased,
        SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0
            END) AS inspire_me_items_purchased_value,
        SUM(CASE
                WHEN item_is_inspire_me = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_ordered,
        SUM(CASE
                WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN 1 ELSE 0
            END) AS initiated_sale_items_purchased,
        SUM(CASE
                WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0
            END) AS initiated_sale_purchased_value,
        item___order_type,
        MAX(time_in_appointment) AS max_time_in_appointment,
        MAX(time_to_appointment) AS max_time_to_appointment,
        order__createdat__dim_date AS order_created_date,
        order_name_merge,
        order__type,
        shipping_address__city,
        shipping_address__postcode,
        SUM(missing) AS number_items_missing,
        SUM(not_available) AS number_items_not_available,
        SUM(out_of_stock) AS number_items_out_of_stock,
        SUM(preorder) AS number_items_preorder,
        SUM(purchased) AS number_items_purchased,
        SUM(qty) AS number_items_ordered,
        SUM(returned) AS number_items_returned,
        through_door_actual,
        tp_actually_ended__dim_date,
        tp_actually_ended__dim_month,
        tp_actually_ended__dim_year,
        tp_actually_started__dim_date,
        tp_actually_started__dim_month,
        tp_actually_started__dim_year,
        try_commission_chargeable,
        try_commission_chargeable_at,
        SUM(item__item_value_pence) AS value_items_ordered,
        SUM(CASE
                WHEN purchased = 1 THEN item__item_value_pence ELSE NULL
            END) AS value_items_purchased,
        SUM(CASE
                WHEN returned = 1 THEN item__item_value_pence ELSE NULL
            END) AS value_items_returned,
        SUM(CASE
                WHEN missing = 1 THEN item__item_value_pence ELSE NULL
            END) AS value_items_missing
    FROM
        order_items o
    GROUP BY
        order_name_merge,
        id_merge,
        brand_name,
        order_cancelled_status,
        order__type,
        item___order_type,
        order__createdat__dim_date,
        order_status,
        customer_type_,
        customer_id,
        discount_total,
        shipping_address__city,
        shipping_address__postcode,
        through_door_actual,
        trial_period,
        tp_actually_ended__dim_date,
        tp_actually_ended__dim_year,
        tp_actually_ended__dim_month,
        tp_actually_started__dim_date,
        tp_actually_started__dim_year,
        tp_actually_started__dim_month,
        try_commission_chargeable,
        try_commission_chargeable_at
    ORDER BY
        order_created_date DESC,
        order__type

WITH NO DATA;

{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_idx ON {{ schema }}.rep__partnership_dashboard_base_view (id_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_order_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (order_name_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_item_order_type_idx ON {{ schema }}.rep__partnership_dashboard_base_view (item___order_type);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_contains_initiated_sale_idx ON {{ schema }}.rep__partnership_dashboard_base_view (contains_initiated_sale);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_dashboard_base_view;
