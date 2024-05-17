{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__currently_on_hold_orders CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__currently_on_hold_orders AS
    SELECT  o.id,
            o.order_name,
            o.order_status,
            o.order_type,
            ois.total_items,
            ois.num_received_by_harper_warehouse,
            get_halo_url(o.id, o.order_type) AS halo_link,
            o.updatedAt,
            o.createdAt,
            {{ clean__order__status_events_columns | prefix_columns('ose', 'orderstatusevent', exclude_columns=['order_id']) }},
            o.airflow_sync_ds
    FROM orders o
    LEFT JOIN useraccount ua ON ua.id = o.style_concierge
    LEFT JOIN clean__order__item__summary ois ON ois.order_id = o.id
    LEFT JOIN clean__order__status_events ose ON ose.order_id = o.id
    WHERE o.order_status='hold' and link_order_child=0
    ORDER BY createdAt DESC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__currently_on_hold_orders_idx ON {{ schema }}.rep__currently_on_hold_orders (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__currently_on_hold_orders;
