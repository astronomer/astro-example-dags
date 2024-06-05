DROP VIEW IF EXISTS {{ schema }}.rep__customer_first_order CASCADE;
CREATE VIEW {{ schema }}.rep__customer_first_order AS
SELECT
    customer_id,
    id,
    createdat,
	DATE(createdat)AS first_order_date
FROM (
    SELECT
        customer_id,
        id,
        createdat,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY createdat) AS rn
    FROM
        public.clean__order__summary
) sub
WHERE rn = 1
