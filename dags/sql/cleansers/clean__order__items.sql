{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__order__items AS
  SELECT
	oi.*,
	oi.order_type AS item__order_type,
	oi.original_order_name AS partner_order_name,
	oi.order_name AS harper_order_name,
	oi.original_name AS product_name,
	CASE
    WHEN POSITION('-' IN oi.original_name) > 0 THEN
        TRIM(SPLIT_PART(oi.original_name, ' - ', 1))
        ELSE oi.original_name
  END AS product_name_cleaned,
	oi.price as item_price_pence,
	oi.discount AS item_discount_price_pence,
	oi.price - oi.discount AS item_value_pence,
	oi.qty AS item_quantity,
	-- changing from bool to int
	-- CASE WHEN oi.purchased = TRUE THEN 1 ELSE 0 END AS purchased,
	-- case WHEN oi.returned = TRUE THEN 1 ELSE 0 END AS returned,
    -- CASE WHEN oi.purchased = TRUE OR oi.returned = TRUE THEN 1 ELSE 0 END AS purchased_originally,
	-- CASE WHEN oi.return_sent_by_customer = TRUE THEN 1 ELSE 0 END AS return_sent,
	-- CASE WHEN oi.received_by_warehouse = TRUE THEN 1 ELSE 0 END AS return_received,
	-- CASE WHEN oi.out_of_stock = TRUE THEN 1 ELSE 0 END AS out_of_stock,
	-- CASE WHEN oi.preorder = TRUE THEN 1 ELSE 0 END AS preorder,
	-- CASE WHEN oi.received = TRUE THEN 1 ELSE 0 END AS received,
	-- CASE WHEN oi.is_initiated_sale = TRUE THEN 1 ELSE 0 END AS is_initiated_sale,
	CASE WHEN oi.order_type = 'inspire_me' THEN 1 ELSE 0 END AS is_inspire_me
FROM {{ schema }}.order__items oi
WHERE
	LOWER(oi.name) NOT LIKE '%%undefined%%'
	AND oi.name IS NOT NULL AND oi.name != ''
	AND oi.order_name IS NOT NULL AND oi.order_name != ''
{% endif %}
