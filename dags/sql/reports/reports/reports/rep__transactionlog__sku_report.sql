DROP VIEW IF EXISTS {{ schema }}.rep__transactionlog__sku_report CASCADE;
CREATE VIEW {{ schema }}.rep__transactionlog__sku_report AS

select
    t.harper_product_type,
    t.transaction_info__payment_at__dim_date,
    t.transaction_info__payment_at__dim_yearcalendarweek_sc,
    t.transaction_info__payment_at__dim_yearmonth_sc,
    t.transaction_info__payment_currency,
    t.partner_name,
    t.lineitem_type,
    t.lineitem_category,
    t.order_type,
    t.harper_order_name,
    t.partner_order_name,
    t.harper_order__customer__first_name,
    t.harper_order__customer__last_name,
    t.calculated_discount,
    t.calculated_discount_code,
    t.calculated_discount_percent,
    t.lineitem_name,
    t.lineitem_billed_quantity,
    t.item_info__sku,
    t.item_info__colour,
    t.item_info__size,
    t.item_info__discount,
    t.harper_order__stripe_customer_link,
    t.harper_order__halo_link,
    t.harper_order__id,
    t.item_info__item_id,
    t.id

FROM rep__transactionlog t
WHERE lineitem_category='product'
ORDER BY t.createdat DESC
