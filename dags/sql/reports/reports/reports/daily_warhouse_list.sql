{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__daily_warehouse_list CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__daily_warehouse_list AS
    SELECT  o.id,
            o.order_name,
            o.order_status,
            o.appointment__date,
            o.style_concierge_name,
            ua.first_name,
            ua.last_name,
            o.order_type,
            r_ois.total_items,
            r_ois.num_received_by_harper_warehouse,
            get_halo_url(o.id, o.order_type) AS halo_link,
            o.updatedAt,
            o.createdAt,
            o.airflow_sync_ds
    FROM orders o
    LEFT JOIN useraccount ua ON ua.id = o.style_concierge
    LEFT JOIN rep__order_item_summary r_ois ON r_ois.order_id = o.id
    WHERE o.appointment__date::date = CURRENT_DATE
      AND o.order_type NOT IN ('add_to_order', 'ship_direct', 'harper_try')
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS daily_warehouse_list_idx ON {{ schema }}.rep__daily_warehouse_list (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__daily_warehouse_list;
