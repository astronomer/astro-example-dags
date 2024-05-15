{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__transaction__summary AS
    SELECT
        t.*,
        tis.*,
        {{ dim__time_columns | prefix_columns('dt', 'payment_at') }}
    FROM
        {{ schema }}.transaction t
    LEFT JOIN {{ schema }}.clean__transaction__item__summary tis ON tis.transaction_id=t.id
    LEFT JOIN {{ schema }}.dim__time dt ON t.payment_at::date = dt.dim_date_id
;

{% endif %}
