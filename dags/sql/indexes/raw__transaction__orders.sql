CREATE INDEX IF NOT EXISTS raw__transaction_orders__transaction_id_idx ON {{ schema }}.raw__transaction__orders (transaction_id);
CREATE INDEX IF NOT EXISTS raw__transaction_orders__order_id_idx ON {{ schema }}.raw__transaction__orders (order_id);
