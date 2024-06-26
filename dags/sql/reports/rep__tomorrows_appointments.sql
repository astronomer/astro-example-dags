{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__tomorrows_appointments CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__tomorrows_appointments AS
    SELECT  o.id,
            o.order_name,
            o.order_status,
            o.appointment__date,
            o.style_concierge_name,
            ua.first_name,
            ua.last_name,
            o.order_type,
            r_ois.total_items,
            r_ois.num_received_by_harper_warehouse,
            o.halo_link,
            o.locate2u_link,
            o.updatedAt,
            o.createdAt,
            o.airflow_sync_ds
    FROM clean__order__summary o
    LEFT JOIN useraccount ua ON ua.id = o.style_concierge
    LEFT JOIN clean__order__item__summary r_ois ON r_ois.order_id = o.id
    WHERE o.appointment__date::date = CURRENT_DATE + INTERVAL '1 day'
      AND o.order_type NOT IN ('ship_direct', 'harper_try')
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__tomorrows_appointments_idx ON {{ schema }}.rep__tomorrows_appointments (id);
{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__tomorrows_appointments;
