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

    ship_directs AS (
        SELECT
            previous_original_order_name,
            id
        FROM
        {{ schema }}.rep__ship_direct_orders
    ),

    order_items AS (
        SELECT
            o.*,
            i.*,
            o.order_type AS order__type,
            i.order_type AS item___order_type,
            o.order_name AS order__name,
            i.order_name AS item__order_name,
            i.createdat AS item__createdat,
            o.createdat AS order__createdat,
            o.createdat__dim_date AS order__createdat__dim_date,
            o.createdat__dim_yearmonth AS order__createdat__dim_yearmonth,
            /*CASE
                WHEN o.ship_direct = 1 THEN sd.initiated_sale__original_order_id
                ELSE o.id
            END AS id_merge,*/
            i.is_initiated_sale AS item_is_initiated_sale,
            i.is_inspire_me AS item_is_inspire_me,
            CASE
                WHEN o.ship_direct = 1 AND (sd.previous_original_order_name IS NOT NULL AND sd.previous_original_order_name != '') THEN sd.previous_original_order_name
                ELSE o.original_order_name
            END AS original_order_name_merge, -- Parent order_name
            i.item_value_pence AS item__item_value_pence
        FROM
            {{ schema }}.rep__deduped_order_items i
        LEFT JOIN
            {{ schema }}.clean__order__summary o ON o.id = i.order_id
        LEFT JOIN
            ship_directs sd ON o.id = sd.id
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
            WHEN order_type = 'harper_try' THEN MAX(tp_actually_ended__dim_date)
            WHEN appointment_completed_at IS NULL THEN MAX(appointment__date__dim_date)
            ELSE DATE(MAX(appointment_completed_at))
            END AS completion_date,
        CASE
            WHEN MAX(CAST(item_is_initiated_sale AS INT)) = 1 THEN 1 ELSE 0
            END AS contains_initiated_sale,
        CASE
            WHEN MAX(CAST(item_is_inspire_me AS INT)) = 1 THEN 1 ELSE 0
        END AS contains_inspire_me,
        customer_id,
        CASE
            WHEN MAX(new_harper_customer) = 1 THEN 'New Harper Customer'
            ELSE 'Returning Harper Customer'
        END AS customer_type_,
        ROUND(CAST(NULLIF(discount_total, ' ') AS NUMERIC) / 100.0, 2) AS discount_total,
		happened,
        harper_product_type,
        --id_merge,
        SUM(CASE
                WHEN item_is_inspire_me = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_ordered,
        SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_purchased,
        ROUND(SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS inspire_me_items_purchased_value,
        SUM(CASE
                WHEN item_is_initiated_sale = 1 THEN 1 ELSE 0
            END) AS initiated_sale_ordered,
        SUM(CASE
                WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN 1 ELSE 0
            END) AS initiated_sale_items_purchased,
        ROUND(SUM(CASE
                WHEN item_is_initiated_sale = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS initiated_sale_purchased_value,
        MAX(time_in_appointment) AS time_in_appointment,
        MAX(time_to_appointment) AS time_to_appointment,
        order__createdat__dim_date AS order_created_date,
        order__createdat__dim_yearmonth,
        order__type,
        original_order_name_merge,
        shipping_address__postcode,
        SUM(missing) AS number_items_missing,
        SUM(not_available) AS number_items_not_available,
        SUM(out_of_stock) AS number_items_out_of_stock,
        SUM(post_purchase_return) AS number_items_post_purchase_return,
        SUM(preorder) AS number_items_preorder,
        SUM(purchased) AS number_items_purchased,
        SUM(qty) AS number_items_ordered,
        SUM(returned) AS number_items_returned,
        SUM(post_purchase_return) AS number_items_post_purchase_return,
        SUM(unpurchased_return) AS number_items_unpurchased_return,
        --through_door_actual,
        tp_actually_ended__dim_date,
        tp_actually_started__dim_date,
        try_commission_chargeable,
        try_commission_chargeable_at,
        ROUND(SUM(item__item_value_pence)/100,2) AS value_items_ordered,
        ROUND(SUM(CASE
                WHEN purchased = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS value_items_purchased,
        ROUND(SUM(CASE
                WHEN returned = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS value_items_returned,
        ROUND(SUM(CASE
                WHEN missing = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS value_items_missing
    FROM
        order_items o
    GROUP BY
        original_order_name_merge,
        --id_merge,
        brand_name,
        order_cancelled_status,
        order__type,
        happened,
        harper_product_type,
        order__createdat__dim_date,
        order__createdat__dim_yearmonth,
        order_status,
        customer_type_,
        customer_id,
        discount_total,
        shipping_address__postcode,
        --through_door_actual,
        trial_period,
        appointment_completed_at,
        tp_actually_ended__dim_date,
        tp_actually_started__dim_date,
        try_commission_chargeable,
        try_commission_chargeable_at

WITH NO DATA;

{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_idx ON {{ schema }}.rep__partnership_dashboard_base_view (id_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_original_order_name_idx ON {{ schema }}.rep__partnership_dashboard_base_view (original_order_name_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_dashboard_base_view_contains_initiated_sale_idx ON {{ schema }}.rep__partnership_dashboard_base_view (contains_initiated_sale);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_dashboard_base_view;
