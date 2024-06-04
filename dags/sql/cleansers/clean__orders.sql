-- CLEANED ORDERS
-- Including first customer order, true customer type, and linked ship direct

-- Extract ship_direct orders
WITH ship_directs AS (
    SELECT
        id,
        order_name,
        createdat,
        initiated_sale__original_order_id
    FROM
        public.orders
    WHERE
        order_type = 'ship_direct'
        AND order_name != ' '
),

-- Link ship_direct orders with their original orders
linked_sd AS (
    SELECT
        sd.id,
        sd.order_name,
        sd.createdat,
        sd.initiated_sale__original_order_id,
        o.order_name AS original_order_name_sd
    FROM
        ship_directs sd
    LEFT JOIN
        public.orders o
    ON
        sd.initiated_sale__original_order_id = o.id
    WHERE
        o.link_order__is_child != 1
),

-- Determine the first order date for each customer
first_order AS (
    SELECT
        customer_id,
<<<<<<< Updated upstream
        MIN(DATE(createdat)) AS first_order_date
=======
        MIN(DATE(createdat)) AS first_order_datest
>>>>>>> Stashed changes
    FROM
        public.orders
    GROUP BY
        customer_id
)

SELECT
    o.*,  -- Include all columns from orders
    fo.first_order_date,
    CASE
        WHEN fo.first_order_date = DATE(o.createdat) THEN 'New Customer'
        ELSE 'Returning Customer'
    END AS customer_type_,
    lsd.original_order_name_sd
FROM
    public.orders o
LEFT JOIN
    linked_sd lsd
ON
    lsd.id = o.id
LEFT JOIN
    first_order fo
ON
    fo.customer_id = o.customer_id;
