CREATE INDEX IF NOT EXISTS raw__transaction_tryons__transaction_id_idx ON {{ schema }}.raw__transaction__try_ons (transaction_id);
CREATE INDEX IF NOT EXISTS raw__transaction_tryons__transactionitem_id_idx ON {{ schema }}.raw__transaction__try_ons (transactionitem_id);
