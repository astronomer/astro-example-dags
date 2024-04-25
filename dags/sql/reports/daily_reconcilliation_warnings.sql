{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__daily_reconcilliation_warnings CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__daily_reconcilliation_warnings AS
    SELECT
        o.id,
        o.order_name,
        o.order_status,
        o.halo_link,
        o.delivery_tracking_urls,
        p.trial_reconciliation_period,
        o.num_return_requested_by_customer,
        o.num_return_sent_by_customer,
        o.num_received_by_partner_warehouse,
        o.return_status,
        o.trial_period_actually_ended_at,
        CURRENT_DATE as current_date,
        (o.trial_period_actually_ended_at::date + p.trial_reconciliation_period * INTERVAL '1 day') as trial_period_reconciliation_due_at,
        (o.trial_period_actually_ended_at::date + p.trial_reconciliation_period * INTERVAL '1 day') - INTERVAL '2 days' as trial_period_reconciliation_warning_due_at
    FROM public.clean__order__summary o
    LEFT JOIN public.partner p ON o.partner_id = p.id
    WHERE
        o.trial_period_actually_ended_at IS NOT NULL
        AND o.num_return_requested_by_customer > 0
        AND o.num_received_by_partner_warehouse=0
        AND CURRENT_DATE >= (o.trial_period_actually_ended_at::date + p.trial_reconciliation_period * INTERVAL '1 day') - INTERVAL '4 days'

    ORDER BY o.trial_period_actually_ended_at DESC
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS daily_reconcilliation_warnings_idx ON {{ schema }}.rep__daily_reconcilliation_warnings (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__daily_reconcilliation_warnings;
