CREATE INDEX IF NOT EXISTS raw__sms_order_id ON {{ schema }}.raw__sms (order_id);
CREATE INDEX IF NOT EXISTS raw__sms_customer_id ON {{ schema }}.raw__sms (customer_id);
