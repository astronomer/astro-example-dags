{% if is_modified %}
DROP FUNCTION IF EXISTS {{ schema}}.get_stripe_fingerprint_url(TEXT, TEXT);
{% endif %}
CREATE OR REPLACE FUNCTION {{ schema}}.get_stripe_fingerprint_url(stripe_payment_fingerprint TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN 'https://dashboard.stripe.com/search??query=fingerprint%%3A' || stripe_payment_fingerprint;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
