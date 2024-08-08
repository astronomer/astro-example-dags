DROP VIEW IF EXISTS {{ schema }}.clean__partners CASCADE;
CREATE VIEW {{ schema }}.clean__partners AS
  SELECT
	p.*,
	{{ dim__time_columns | prefix_columns('pc', 'createdat') }}
FROM {{ schema }}.partner p
LEFT JOIN
    {{ schema }}.dim__time pc ON p.createdat::date = pc.dim_date_id
;
