{% if is_modified %}
DROP MATERIALIZED VIEW IF EXISTS {{ schema }}.rep__deduped_order_items CASCADE;
{% endif %}
-- We have to do this because we have bad data which results in items duplicated in orders
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ schema }}.rep__deduped_order_items AS
WITH RankedOrderItems AS (
  SELECT
    i.*,
    ROW_NUMBER() OVER (PARTITION BY i.item_id, i.is_link_order_child_item ORDER BY i.updatedat DESC) AS rn
  FROM {{ schema }}.clean__order__items i
)
SELECT
  *
FROM RankedOrderItems
WHERE rn = 1;

{% if is_modified %}
CREATE UNIQUE INDEX IF NOT EXISTS rep__dedup_order_items_idx ON {{ schema }}.rep__deduped_order_items (id);
CREATE INDEX IF NOT EXISTS rep__dedup_order_items_harper_order_name ON {{ schema }}.rep__deduped_order_items (harper_order_name);
CREATE INDEX IF NOT EXISTS rep__dedup_order_items_partner_order_name ON {{ schema }}.rep__deduped_order_items (partner_order_name);
CREATE INDEX IF NOT EXISTS rep__dedup_order_items_translookup ON {{ schema }}.rep__deduped_order_items (item_id, is_link_order_child_item, order_id);
{% endif %}
