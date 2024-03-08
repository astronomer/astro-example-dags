{% if is_modified %}
DROP FUNCTION IF EXISTS get_halo_url(TEXT, TEXT);
{% endif %}
CREATE OR REPLACE FUNCTION get_halo_url(order_id TEXT, order_type TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN 'https://harper-admin.harperconcierge.com/' ||
        CASE
            WHEN order_type = 'harper_try' THEN 'order'
            ELSE 'appointment'
        END || '/' || order_id;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
