{% if is_modified %}
CREATE OR REPLACE VIEW {{ schema }}.clean__transaction__items AS
    SELECT
        tp.transaction_id,
        tp.transactionitem_id,
        'purchase' AS transaction_type,
        tpi.*
    FROM transaction__purchases tp
    LEFT JOIN transactionitem tpi ON tpi.id = tp.transactionitem_id

    UNION ALL

    SELECT
        tr.transaction_id,
        tr.transactionitem_id,
        'refund' AS transaction_type,
        tri.*
    FROM transaction__refunds tr
    LEFT JOIN transactionitem tri ON tri.id = tr.transactionitem_id

    UNION ALL

    SELECT
        tt.transaction_id,
        tt.transactionitem_id,
        'try_on' AS transaction_type,
        tti.*
    FROM transaction__try_ons tt
    LEFT JOIN transactionitem tti ON tti.id = tt.transactionitem_id

    UNION ALL

    SELECT
        td.transaction_id,
        td.transactionitem_id,
        'discount' AS transaction_type,
        tdi.*
    FROM transaction__discounts td
    LEFT JOIN transactionitem tdi ON tdi.id = td.transactionitem_id;
{% endif %}
