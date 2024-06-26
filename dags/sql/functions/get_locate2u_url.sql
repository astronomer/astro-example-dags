{% if is_modified %}
DROP FUNCTION IF EXISTS {{ schema }}.get_locate2u_url(TEXT) CASCADE;
{% endif %}
CREATE OR REPLACE FUNCTION {{ schema }}.get_locate2u_url(locate2u_stop_id TEXT) RETURNS TEXT AS $$
BEGIN
    IF locate2u_stop_id IS NULL OR locate2u_stop_id = '' THEN
        RETURN NULL;
    ELSE
        RETURN 'https://app.locate2u.com/stops/details/' || locate2u_stop_id;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
