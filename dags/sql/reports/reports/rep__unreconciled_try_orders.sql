{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__unreconciled_try_orders CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__unreconciled_try_orders AS

    SELECT
        CASE
          WHEN o.itemsummary__num_received_by_partner_warehouse < o.itemsummary__total_items
            THEN 'true'
            ELSE 'false'
        END AS needs_invoicing,
        o.order_name,
        o.order_status,
        o.brand_name,
        o.fulfillment_status,
        o.createdat,
        p.trial_reconciliation_period,
        o.itemsummary__total_items,
        o.itemsummary__num_purchased,
        o.itemsummary__num_actually_purchased,
        o.itemsummary__num_received_by_partner_warehouse,
        o.itemsummary__num_return_requested_by_customer,
        o.halo_link,
        o.number_of_items_received,
        o.number_of_items_purchased,
        o.number_of_items_refunded,
        o.trial_period_start_at,
        o.orderstatusevent__trialperiodstarted_at,
        o.trial_period_actually_started_at,
        o.trial_period_end_at,
        o.trial_period_actually_ended_at,
        o.orderstatusevent__trialperiodended_at,
        o.orderstatusevent__trialperiodfinalreconciliation_at,
        o.trial_period_actually_reconciled_at,
        o.id
    FROM {{ schema }}.rep__order__summary o
    LEFT JOIN clean__partners p ON p.id = o.partner_id

    WHERE
        o.orderstatusevent__trialperiodfinalreconciliation_at IS NULL
        AND o.createdat < CURRENT_DATE - INTERVAL '1 day' * (p.trial_period + p.trial_reconciliation_period)
        AND o.fulfillment_status NOT IN ('fulfilled', 'unfulfilled')
        AND o.order_status NOT IN ('cancelled', 'in_try_on_period') AND order_type = 'harper_try'
        AND o.itemsummary__num_received_by_partner_warehouse < itemsummary__total_items
    ORDER BY needs_invoicing DESC, o.brand_name, o.createdat ASC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__unreconciled_try_orders_idx ON {{ schema }}.rep__unreconciled_try_orders (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__unreconciled_try_orders;
