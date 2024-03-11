-- RUN THIS MANUALLY
--CREATE USER tableau WITH PASSWORD 'xxxxxxxxx';
{% if is_modified %}
-- Grant connect on the "datalake" database
GRANT CONNECT ON DATABASE datalake TO tableau;

-- Grant usage on the public schema
GRANT USAGE ON SCHEMA public TO tableau;

-- Set default privileges for tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO tableau;

-- Set default privileges for sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO tableau;

-- Set default privileges for functions
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO tableau;

-- Grant select on all existing tables in the public schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tableau;

-- Grant select on all sequences in the public schema
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO tableau;

-- Grant execute on all functions in the public schema
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO tableau;
{% else %}
NULL;
{% endif %}
