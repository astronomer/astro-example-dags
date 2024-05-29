CREATE INDEX IF NOT EXISTS raw__transactionitem__item_id_idx ON {{ schema }}.raw__transactionitem (item_id);
CREATE INDEX IF NOT EXISTS raw__transactionitem__order_idx ON {{ schema }}.raw__transactionitem (order_id);
CREATE INDEX IF NOT EXISTS raw__transactionitem__lineitem_category_idx ON {{ schema }}.raw__transactionitem (lineitem_category);
CREATE INDEX IF NOT EXISTS raw__transactionitem__lineitem_type_idx ON {{ schema }}.raw__transactionitem (lineitem_type);
