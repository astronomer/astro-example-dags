CREATE INDEX IF NOT EXISTS raw__transaction_discounts__transaction_id_idx ON {{ schema }}.raw__transaction__discounts (transaction_id);
CREATE INDEX IF NOT EXISTS raw__transaction_discounts__transactionitem_id_idx ON {{ schema }}.raw__transaction__discounts (transactionitem_id);
