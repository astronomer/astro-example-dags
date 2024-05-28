{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__transactionlog CASCADE;
{% endif %}

CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__transactionlog AS
    SELECT
        ho.brand_name AS partner_name,
        ho.try_commission_chargeable as try_commission_chargeable,
        ho.try_commission_chargeable_at as try_commission_chargeable_at,
        ti.*,
        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__discount_applied
        ELSE
           0
        END AS order_discount_in_appointment_discount_applied,

        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__discount_amount
        ELSE
            NULL
        END AS order_discount_in_appointment_discount_amount,

        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__absorbed_by
        ELSE
            ''
        END AS order_discount_in_appointment_absorbed_by,

        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__reason
        ELSE
            ''
        END AS order_discount_in_appointment_reason,

        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__discount_code
        ELSE
            ''
        END AS order_discount_in_appointment_discount_code,

        CASE WHEN t.lineitem_type = 'discount' THEN
            ho.discount_in_appointment__discount_type
        ELSE
            ''
        END AS order_discount_in_appointment_discount_type,

        i.partner_order_name as partner_order_name,
        i.harper_order_name as harper_order_name,
        i.commission__commission_type as commission_type,
        i.commission__percentage as commission_percentage,
        i.commission__calculated_amount as commission_amount_calculated,
        i.is_initiated_sale as is_initiated_sale,
        i.initiated_sale__initiated_sale_type as initiated_sale_type,
        i.initiated_sale__original_order_id as initiated_sale_original_order_id,
        i.initiated_sale__user_email as initiated_sale_user_email,
        i.initiated_sale__user_role as initiated_sale_user_role,
        i.is_inspire_me as is_inspire_me,
        i.initiated_sale__inspire_me_option_selected as inspire_me_option_selected,
        i.initiated_sale__inspire_me_description as inspire_me_description,
        i.order_type AS order_type,
        {{ clean__transaction__summary_columns | prefix_columns('t', 'transaction_info', exclude_columns=[]) }},
        {{ rep__deduped_order_items_columns | prefix_columns('i', 'item_info') }},
        {{ rep__order__summary_columns | prefix_columns('hos', 'harper_order') }}
    FROM
        {{ schema }}.clean__transaction__items ti
    LEFT JOIN {{ schema }}.clean__transaction__summary t ON t.id = ti.transaction_id
    LEFT JOIN {{ schema }}.rep__order__summary hos ON ti.order_id = hos.id
    LEFT JOIN {{ schema }}.rep__deduped_order_items i on ti.item_id = i.item_id AND i.is_link_order_child_item = 0 AND i.order_id=ti.order

    ORDER BY t.createdat ASC
WITH NO DATA;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__transactionlog_idx ON {{ schema }}.rep__transactionlog (id);
CREATE INDEX IF NOT EXISTS rep__transactionlog_harper_order_name_idx ON {{ schema }}.rep__transactionlog (harper_order_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_partner_order_name_idx ON {{ schema }}.rep__transactionlog (partner_order_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_try_commission_chargeable_idx ON {{ schema }}.rep__transactionlog (try_commission_chargeable);

CREATE INDEX IF NOT EXISTS rep__transactionlog_is_initiated_sale_idx ON {{ schema }}.rep__transactionlog (is_initiated_sale);
CREATE INDEX IF NOT EXISTS rep__transactionlog_is_inspire_me_idx ON {{ schema }}.rep__transactionlog (is_inspire_me);
CREATE INDEX IF NOT EXISTS rep__transactionlog_order_type_idx ON {{ schema }}.rep__transactionlog (order_type);

{% endif %}

REFRESH MATERIALIZED VIEW {{ schema }}.rep__transactionlog;
