/*Partnership Dashboard Orders
1) CTE for first order with Harper for each customer
2) Item level report for the join of orders__items and orders
3) Two reports from (2): one grouped by order (linked to original order)
	and one grouped by item.order_type to analyze order makeup and split into parts (e.g., initiated sales)
To add: first time harper use with brand*/
{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__partnership_metric_summary CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__partnership_metric_summary AS
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
            o.createdat__dim_month AS order__createdat__dim_month,
            o.createdat__dim_year AS order__createdat__dim_year,
            --i.initiated_sale__user_role AS item__initiated_sale__user_role,
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
        appointment__date__dim_month,
        appointment__date__dim_year,
        order__createdat__dim_month,
        order__createdat__dim_year,
        brand_name,
        order__type,
        happened,
        harper_product_type,
        SUM(success)
        SUM(new_harper_customer) AS new_harper_customers,
        SUM(CASE
                WHEN item_is_inspire_me = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_ordered,
        ROUND(SUM(CASE
                WHEN item_is_inspire_me = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS inspire_me_items_ordered_value,
        SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN 1 ELSE 0
            END) AS inspire_me_items_purchased,
        ROUND(SUM(CASE
                WHEN item_is_inspire_me = 1 AND purchased = 1 THEN item__item_value_pence ELSE 0
            END)/100,2) AS inspire_me_items_purchased_value,
        SUM(time_in_appointment) AS total_time_in_appointment,
        SUM(time_to_appointment) AS total_time_to_appointment,
        SUM(itemsummary__num_items_ordered) AS num_items_ordered,
        SUM(itemsummary__num_items_fulfilled) AS num_items_fulfilled,
        SUM(itemsummary__num_purchased) AS num_purchased,
        SUM(itemsummary__num_returned) AS num_returned,
        SUM(itemsummary__num_purchased_net) AS num_purchased_net,
        SUM(itemsummary__num_actually_purchased) AS num_actually_purchased, -- duplicate of num_purchased
        SUM(itemsummary__num_preorder) AS num_preorder,
        SUM(itemsummary__num_received_by_harper_warehouse) AS num_received_by_harper_warehouse,
        SUM(itemsummary__num_received_by_partner_warehouse) AS num_received_by_partner_warehouse,
        SUM(itemsummary__num_return_requested_by_customer) AS num_return_requested_by_customer,
        SUM(itemsummary__num_return_sent_by_customer) AS num_return_sent_by_customer,

        -- Value Summary
        SUM(itemsummary__total_value_ordered) AS total_value_ordered,
        SUM(itemsummary__total_value_purchased) AS total_value_purchased,
        SUM(itemsummary__total_value_returned) AS total_value_returned,
        SUM(itemsummary__total_value_received) AS total_value_received,
        SUM(itemsummary__total_value_purchased_net) AS total_value_purchased_net,
        SUM(itemsummary__total_value_received_by_warehouse) AS total_value_received_by_warehouse,

        -- Initiated Sale Summary
        SUM(itemsummary__initiated_sale__num_ordered) AS initiated_sale__num_ordered,
        SUM(itemsummary__initiated_sale__num_items_fulfilled) AS initiated_sale__num_items_fulfilled,
        SUM(itemsummary__initiated_sale__num_purchased) AS initiated_sale__num_purchased,
        SUM(itemsummary__initiated_sale__num_returned) AS initiated_sale__num_returned,
        SUM(itemsummary__initiated_sale__num_actually_purchased) AS initiated_sale__num_actually_purchased,
        SUM(itemsummary__initiated_sale__num_preorder) AS initiated_sale__num_preorder,
        SUM(itemsummary__initiated_sale__num_received_by_harper_warehouse) AS initiated_sale__num_received_by_harper_warehouse,
        SUM(itemsummary__initiated_sale__num_received_by_partner_warehouse) AS initiated_sale__num_received_by_partner_warehouse,
        SUM(itemsummary__initiated_sale__num_return_requested_by_customer) AS initiated_sale__num_return_requested_by_customer,
        SUM(itemsummary__initiated_sale__num_return_sent_by_customer) AS initiated_sale__num_return_sent_by_customer,

        -- Initiated Sale Value Summary
        SUM(itemsummary__initiated_sale__total_value_ordered) AS initiated_sale__total_value_ordered,
        SUM(itemsummary__initiated_sale__total_value_purchased) AS initiated_sale__total_value_purchased,
        SUM(itemsummary__initiated_sale__total_value_returned) AS initiated_sale__total_value_returned,
        SUM(itemsummary__initiated_sale__total_value_received) AS initiated_sale__total_value_received,
        SUM(itemsummary__initiated_sale__total_value_received_by_warehouse) AS initiated_sale__total_value_received_by_warehouse

    FROM
        order_items o
    GROUP BY
        appointment__date__dim_month,
        appointment__date__dim_year,
        order__createdat__dim_month,
        order__createdat__dim_year,
        brand_name,
        order__type,
        happened,
        harper_product_type

WITH NO DATA;

{% if is_modified %}
--CREATE UNIQUE INDEX IF NOT EXISTS rep__partnership_metric_summary_idx ON {{ schema }}.rep__partnership_metric_summary (id_merge);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_brand_name ON {{ schema }}.rep__partnership_metric_summary (brand_name);
CREATE INDEX IF NOT EXISTS rep__partnership_metric_summary_order__type ON {{ schema }}.rep__partnership_metric_summary (order__type);


{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__partnership_metric_summary;
