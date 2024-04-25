{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__order_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__order_summary AS
    SELECT
        o.*,
        get_halo_url(o.id, o.order_type) AS halo_link,
        r__ois.*,
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
        rep__order_item_summary r__ois ON r__ois.order_id = o.id
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__order_summary_idx ON {{ schema }}.rep__order_summary (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__order_summary;
