CREATE UNIQUE INDEX IF NOT EXISTS raw__orders__apptdate_idx ON {{ schema }}.raw__orders (appointment__date);
