{% if is_modified %}
DROP FUNCTION IF EXISTS {{ schema}}.get_stripe_customer_url(TEXT, TEXT) CASCADE;
{% endif %}
CREATE OR REPLACE FUNCTION {{ schema}}.get_stripe_customer_url(stripe_customer_id TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN 'https://dashboard.stripe.com/customers/' || stripe_customer_id;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
