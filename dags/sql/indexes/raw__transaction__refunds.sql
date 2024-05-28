CREATE INDEX IF NOT EXISTS raw__transaction_refunds__transaction_id_idx ON {{ schema }}.raw__transaction__refunds (transaction_id);
CREATE INDEX IF NOT EXISTS raw__transaction_refunds__transactionitem_id_idx ON {{ schema }}.raw__transaction__refunds (transactionitem_id);
