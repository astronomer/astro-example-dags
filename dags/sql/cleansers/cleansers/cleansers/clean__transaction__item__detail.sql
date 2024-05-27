{% if is_modified %}
DROP VIEW IF EXISTS {{ schema }}.clean__transaction__item__detail CASCADE;
CREATE VIEW {{ schema }}.clean__transaction__item__detail AS
    SELECT
        ti.*,
        i.partner_order_name as partner_order_name,
        o.order_name as harper_order_name,
        {{ clean__order__summary_columns | prefix_columns('o', 'order_info') }},
        {{ clean__order__items_columns | prefix_columns('i', 'item_info') }}
    FROM
        {{ schema }}.clean__transaction__items ti
    LEFT JOIN {{ schema }}.clean__order__summary o on ti.order = o.id
    LEFT JOIN {{ schema }}.clean__order__items i on ti.item_id = i.item_id AND ti.order = i.mongo_id

;

{% endif %}
