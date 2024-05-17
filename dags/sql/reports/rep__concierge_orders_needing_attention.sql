{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__concierge_orders_needing_attention CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__concierge_orders_needing_attention AS
    SELECT
            o.id,
            o.order_name,
            o.brand_name,
            o.order_status,
            o.order_type,
            o.itemsummary__total_items,
            o.itemsummary__num_received_by_harper_warehouse,
            c.first_name,
            c.last_name,
            c.total_orders as total_previous_orders,
            o.appointment__date,
            o.appointment_completed_at,
            o.halo_link,
            o.stripe_customer_link,
            o.updatedAt,
            o.createdAt,
            {{ clean__order__status_events_columns | unprefix_columns('o', 'orderstatusevent', exclude_columns=['order_id']) }},
            o.airflow_sync_ds
    FROM clean__order__summary o
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.order_status IN ('hold', 'in_resolution', 'return_required') AND o.is_harper_try=0 and link_order_child=0
    ORDER BY createdAt DESC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__concierge_orders_needing_attention_idx ON {{ schema }}.rep__concierge_orders_needing_attention (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__concierge_orders_needing_attention;
