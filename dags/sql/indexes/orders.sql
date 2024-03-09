CREATE UNIQUE INDEX IF NOT EXISTS orders__apptdate_idx ON {{ schema }}.orders (appointment__date);
