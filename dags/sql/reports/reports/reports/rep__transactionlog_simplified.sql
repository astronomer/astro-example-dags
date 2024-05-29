{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__transactionlog_simplified CASCADE;
{% endif %}
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__transactionlog_simplified AS

SELECT
    t.transaction_info__payment_at__dim_date,
    t.transaction_info__payment_at__dim_yearcalendarweek_sc,
    -- t.transaction_info__payment_at__dim_dim_yearmonth_sc,
    t.transaction_info__payment_reference,
    t.transaction_info__payment_reference_id,
    t.transaction_info__payment_provider,
    t.transaction_info__payment_currency,
    t.lineitem_type,
    t.lineitem_category,
    t.partner_name,
    t.harper_product_type,
    t.order_type,
    t.harper_order__order_status,
    t.harper_order_name,
    t.partner_order_name,
    t.harper_order__createdat,
    --t.harper_order__createdat__dim_date,
    --t.harper_order__createdat__dim_yearcalendarweek_sc,
    --dim_yearmonth_sc
    t.harper_order__customer__first_name,
    t.harper_order__customer__last_name,
    t.transaction_info__total_amount,
    t.transaction_info__payment_invoiced_amount,
    t.transaction_info__item_count,
    t.harper_order__waive_reason,
    t.calculated_discount,
    t.calculated_discount_code,
    t.calculated_discount_percent,
    t.lineitem_name,
    t.lineitem_billed_quantity,
    t.item_info__sku,
    t.item_info__colour,
    t.item_info__size,
    t.item_info__billed_qty,
    t.item_info__discount,
    t.initiated_sale_type,
    t.initiated_sale_user_email,
    t.initiated_sale_user_role,
    t.item_info__commission__calculated_amount AS commission_calculated_amount_exc_vat,
    t.harper_order__style_concierge_name,
    t.try_commission_chargeable,
    t.try_commission_chargeable_at,
    --t.try_commission_chargeable_at__dim_date,
    --t.try_commission_chargeable_at__dim_yearcalendarweek_sc,
    --dim_yearmonth_sc
    t.transaction_info__payment_at,
    t.id

FROM
  rep__transactionlog t

ORDER BY
  t.createdat DESC
WITH NO DATA;
{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__transactionlog_simplified_idx ON {{ schema }}.rep__transactionlog_simplified (id);
CREATE INDEX IF NOT EXISTS rep__transactionlog_try_commission_chargeable_idx ON {{ schema }}.rep__transactionlog_simplified (try_commission_chargeable);
CREATE INDEX IF NOT EXISTS rep__transactionlog_try_payment_at_idx ON {{ schema }}.rep__transactionlog_simplified (transaction_info__payment_at);
CREATE INDEX IF NOT EXISTS rep__transactionlog_order_type_idx ON {{ schema }}.rep__transactionlog_simplified (order_type);
CREATE INDEX IF NOT EXISTS rep__transactionlog_partner_name_idx ON {{ schema }}.rep__transactionlog_simplified (partner_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_partner_order_name_idx ON {{ schema }}.rep__transactionlog_simplified (partner_order_name);
CREATE INDEX IF NOT EXISTS rep__transactionlog_harper_order_name_idx ON {{ schema }}.rep__transactionlog_simplified (harper_order_name);
{% endif %}
REFRESH MATERIALIZED VIEW {{ schema }}.rep__transactionlog_simplified;
