{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ destination_schema }}.daily_warehouse_list CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ destination_schema }}.daily_warehouse_list AS
    SELECT  o.id,
            o.order_name,
            o.order_status,
            o.appointment__date,
            o.style_concierge_name,
            ua.first_name,
            ua.last_name,
            o.order_type,
            r_ois.*,
            o.updatedAt,
            o.createdAt,
            o.airflow_sync_ds
    FROM orders o
    LEFT JOIN useraccount ua ON ua.id = o.style_concierge
    LEFT JOIN report_order_item_summary r_ois ON r_ois.order_id = o.id
    WHERE o.appointment__date::date = CURRENT_DATE
      AND o.order_type NOT IN ('add_to_order', 'ship_direct', 'harper_try')
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX daily_warehouse_list_idx ON {{ destination_schema }}.daily_warehouse_list (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ destination_schema }}.daily_warehouse_list;
