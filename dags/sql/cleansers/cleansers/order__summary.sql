{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__order__summary AS
    SELECT
        o.*,
        get_halo_url(o.id, o.order_type) AS halo_link,
        clean__ois.*,
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
    ;
{% endif %}
