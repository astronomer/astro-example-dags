{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__currently_on_hold_orders_harper_try CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__currently_on_hold_orders_harper_try AS
    SELECT  o.id,
            o.order_name,
            o.order_status,
            o.order_type,
            ois.total_items,
            ois.num_received_by_harper_warehouse,
            c.first_name,
            c.last_name,
            c.total_orders,
            o.trial_period_start_at,
            o.trial_period_actually_started_at,
            o.trial_period_end_at,
            o.trial_period_actually_ended_at,
            o.trial_period_actually_reconciled_at,
            get_halo_url(o.id, o.order_type) AS halo_link,
            get_stripe_customer_url(c.stripe_customer_id) AS stripe_customer,
            o.updatedAt,
            o.createdAt,
            {{ clean__order__status_events_columns | prefix_columns('ose', 'orderstatusevent', exclude_columns=['order_id']) }},
            o.airflow_sync_ds
    FROM orders o
    LEFT JOIN useraccount ua ON ua.id = o.style_concierge
    LEFT JOIN clean__order__item__summary ois ON ois.order_id = o.id
    LEFT JOIN clean__order__status_events ose ON ose.order_id = o.id
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.order_status='hold' AND o.is_harper_try=True and link_order_child=0
    ORDER BY createdAt DESC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__currently_on_hold_orders_harper_try_idx ON {{ schema }}.rep__currently_on_hold_orders_harper_try (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__currently_on_hold_orders;
