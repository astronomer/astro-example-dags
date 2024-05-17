{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__unfulfilled_harper_try_orders CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__unfulfilled_harper_try_orders AS
    SELECT
            o.id,
            o.order_name,
            o.brand_name,
            o.order_status,
            o.fulfillment_status,
            (CURRENT_DATE - o.createdat::date) as days_old,
            CURRENT_DATE as current_date,
            o.createdAt,
            o.updatedAt,
            o.shipping_method__name,
            o.shipping_method__description,
            o.itemsummary__total_items,
            o.itemsummary__num_received_by_harper_warehouse,
            c.first_name,
            c.last_name,
            c.total_orders as total_previous_orders,
            o.halo_link,
            o.stripe_customer_link,
            o.order_type,
            o.trial_period_start_at,
            o.trial_period_actually_started_at,
            o.trial_period_end_at,
            o.trial_period_actually_ended_at,
            o.trial_period_actually_reconciled_at,
            {{ clean__order__status_events_columns | unprefix_columns('o', 'orderstatusevent', exclude_columns=['order_id']) }},
            o.airflow_sync_ds
    FROM clean__order__summary o
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.order_status='new' AND o.is_harper_try=1 and link_order_child=0
    AND (o.fulfillment_status IS NULL OR o.fulfillment_status = 'unfulfilled')
    AND (CURRENT_DATE - o.createdat::date) > 1
    ORDER BY createdAt ASC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__unfulfilled_harper_try_orders_idx ON {{ schema }}.rep__unfulfilled_harper_try_orders (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__unfulfilled_harper_try_orders;
