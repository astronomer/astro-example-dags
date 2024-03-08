{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__order_summary CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__order_summary AS
    SELECT
        o.*,
        get_halo_url(o.id, o.order_type) AS halo_link,
        r__ois.*,
        adt.*
    FROM
        {{ schema }}.orders o
    LEFT JOIN
        dim__time adt ON o.appointment__date::date = adt.date_id
    LEFT JOIN
        rep__order_item_summary r__ois ON r__ois.order_id = o.id
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__order_summary_idx ON {{ schema }}.rep__order_summary (id);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__order_summary;
