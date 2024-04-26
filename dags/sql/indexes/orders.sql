CREATE INDEX IF NOT EXISTS raw__orders__apptdate_idx ON {{ schema }}.raw__orders (appointment__date);
CREATE INDEX IF NOT EXISTS raw__orders_brand_name_idx ON {{ schema }}.raw__orders (brand_name);
CREATE INDEX IF NOT EXISTS raw__orders_order_status_idx ON {{ schema }}.raw__orders (order_status);
CREATE INDEX IF NOT EXISTS raw__orders_createdat_idx ON {{ schema }}.raw__orders (createdat);
