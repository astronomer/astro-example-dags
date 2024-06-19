DROP VIEW IF EXISTS {{ schema }}.clean__order__summary CASCADE;
CREATE VIEW {{ schema }}.clean__order__summary AS
    SELECT
        o.*,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.createdat) AS customer_order_seq,
        CASE WHEN o.order_type IN ('harper_try') THEN
            'harper_try'
        ELSE
            'harper_concierge'
        END AS harper_product_type,
        get_halo_url(o.id, o.order_type) AS halo_link,
        get_stripe_customer_url(c.stripe_customer_id) AS stripe_customer_link,
        {{ clean__order__item__summary_columns | prefix_columns('clean__ois', 'itemsummary', exclude_columns=['order_id']) }},
        {{ clean__order__status_events_columns | prefix_columns('clean__ose', 'orderstatusevent', exclude_columns=['order_id']) }},
        {{ dim__time_columns | prefix_columns('adt', 'appointment__date') }},
        {{ dim__time_columns | prefix_columns('tas', 'tp_actually_started') }},
        {{ dim__time_columns | prefix_columns('tae', 'tp_actually_ended') }},
        {{ dim__time_columns | prefix_columns('tar', 'tp_actually_reconciled') }},
        {{ dim__time_columns | prefix_columns('tcc', 'try_chargeable_at') }},
        {{ dim__time_columns | prefix_columns('oc', 'createdat') }}
    FROM
        {{ schema }}.orders o
    LEFT JOIN
        dim__time adt ON o.appointment__date::date = adt.dim_date_id
    LEFT JOIN
        dim__time tas ON o.trial_period_actually_started_at::date = tas.dim_date_id
    LEFT JOIN
        dim__time tae ON o.trial_period_actually_ended_at::date = tae.dim_date_id
    LEFT JOIN
        dim__time tar ON o.trial_period_actually_reconciled_at::date = tar.dim_date_id
    LEFT JOIN
        dim__time tcc ON o.try_commission_chargeable_at::date = tcc.dim_date_id
    LEFT JOIN
        dim__time oc ON o.createdat::date = oc.dim_date_id
    LEFT JOIN
        clean__order__item__summary clean__ois ON clean__ois.order_id = o.id
    LEFT JOIN
        clean__order__status_events clean__ose ON clean__ose.order_id = o.id
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.brand_name IS NOT NULL
    AND o.brand_name NOT IN ('ME+EM UAT', 'Harper UAT Shopify','',' ')
    AND o.order_name IS NOT NULL
    AND o.order_name NOT IN ('',' ','  ',' -L1')
    AND lower(o.customer__first_name) NOT LIKE '%%test%%'
    ;
