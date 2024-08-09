{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__trial_period_not_ended CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__trial_period_not_ended AS
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
        o.itemsummary__total_items,
        o.itemsummary__num_purchased,
        o.itemsummary__num_actually_purchased,
        o.itemsummary__num_received_by_partner_warehouse,
        o.itemsummary__num_return_requested_by_customer,
        o.halo_link,
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

    WHERE
        (o.orderstatusevent__trialperiodended_at IS NULL OR o.orderstatusevent__trialperiodstarted_at IS NULL)
        AND CURRENT_DATE > o.trial_period_end_at
        AND o.order_status NOT IN ('cancelled') AND order_type = 'harper_try'
    ORDER BY needs_invoicing DESC, o.brand_name, o.createdat ASC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__trial_period_not_ended_idx ON {{ schema }}.rep__trial_period_not_ended (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__trial_period_not_ended;
