{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__order__summary AS
    SELECT
        o.*,
        get_halo_url(o.id, o.order_type) AS halo_link,
        get_stripe_customer_url(c.stripe_customer_id) AS stripe_customer_link,
        {{ clean__order__item__summary_columns | prefix_columns('clean__ois', 'itemsummary', exclude_columns=['order_id']) }},
        {{ clean__order__status_events_columns | prefix_columns('clean__ose', 'orderstatusevent', exclude_columns=['order_id']) }},

        {{ dim__time_columns | prefix_columns('adt', 'appointment__date') }},
        {{ dim__time_columns | prefix_columns('tas', 'tp_actually_started') }},
        {{ dim__time_columns | prefix_columns('tae', 'tp_actually_ended') }},
        {{ dim__time_columns | prefix_columns('tar', 'tp_actually_reconciled') }}
    FROM
        {{ schema }}.orders o
    LEFT JOIN
        dim__time adt ON o.appointment__date::date = adt.dim_date_id
    LEFT JOIN
        dim__time tas ON o.trial_period_actually_started_at::date = tas.dim_date_id
    LEFT JOIN
        dim__time tae ON o.trial_period_actually_ended_at::date = tae.dim_date_id
    LEFT JOIN
        dim__time tar ON o.trial_period_actually_reconciled_at::date = tar.dim_date_id
    LEFT JOIN
        clean__order__item__summary clean__ois ON clean__ois.order_id = o.id
    LEFT JOIN
        clean__order__status_events clean__ose ON clean__ose.order_id = o.id
    LEFT JOIN customer c ON c.id = o.customer_id

    ;
{% endif %}
