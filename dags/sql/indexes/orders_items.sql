CREATE INDEX IF NOT EXISTS raw__order_items_order_id_idx ON {{ schema }}.raw__order__items (order_id);
CREATE INDEX IF NOT EXISTS raw__order_items_name_idx ON {{ schema }}.raw__order__items (name);
CREATE INDEX IF NOT EXISTS raw__order_items_order_name_idx ON {{ schema }}.raw__order__items (order_name);
