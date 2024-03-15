{% if is_modified %}
DROP TABLE IF EXISTS {{ schema}}.dim__time CASCADE;
{% endif %}
DO $$
DECLARE
    -- Variable to hold whether the table was just created (false initially)
    table_was_created BOOLEAN := FALSE;
BEGIN
    -- Check if the table exists
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = 'dim__time') THEN
        -- Create the table if it does not exist
        CREATE TABLE public.dim__time (
            dim_date_id DATE PRIMARY KEY,
            dim_date DATE,
            dim_year INT,
            dim_month INT,
            dim_dayofmonth INT,
            dim_dayofyear INT,
            dim_calendarweek INT,
            dim_formatteddate TEXT,
            dim_quartal TEXT,
            dim_yearquartal TEXT,
            dim_yearmonth TEXT,
            dim_yearmonth_sheets_compatible TEXT,
            dim_yearcalendarweek TEXT,
            dim_yearcalendarweek_sheets_compatible TEXT,
            dim_dayofweek TEXT,
            dim_dayofweek_num INT,
            dim_isodayofweek_num INT,
            dim_weekend TEXT,
            dim_monthstart DATE,
            dim_monthend DATE
        );

        -- Set the flag since the table was just created
        table_was_created := TRUE;

        -- Create an index on the id column if the table was just created
        CREATE INDEX IF NOT EXISTS idx_dim__time_id ON public.dim__time(dim_date_id);
    END IF;

    -- If the table was just created, perform the INSERT operation
    IF table_was_created THEN
        INSERT INTO dim__time (
            dim_date_id,
            dim_date,
            dim_year,
            dim_month,
            dim_dayofmonth,
            dim_dayofyear,
            dim_calendarweek,
            dim_formatteddate,
            dim_quartal,
            dim_yearquartal,
            dim_yearmonth,
            dim_yearmonth_sheets_compatible,
            dim_yearcalendarweek,
            dim_yearcalendarweek_sheets_compatible,
            dim_dayofweek,
            dim_dayofweek_num,
            dim_isodayofweek_num,
            dim_weekend,
            dim_monthstart,
            dim_monthend
        )
        SELECT
            datum as dim_date_id,
            datum as dim_date,
            EXTRACT(YEAR FROM datum) as dim_year,
            EXTRACT(MONTH FROM datum) as dim_month,
            EXTRACT(DAY FROM datum) as dim_dayofmonth,
            EXTRACT(DOY FROM datum) as dim_dayofyear,
            EXTRACT(WEEK FROM datum) as dim_calendarweek,
            TO_CHAR(datum, 'yyyy-mm-dd') as dim_formatteddate,
            'q' || TO_CHAR(datum, 'q') as dim_quartal,
            TO_CHAR(datum, 'yyyy/"q"q') as dim_yearquartal,
            TO_CHAR(datum, 'yyyy/mm') as dim_yearmonth,
            TO_CHAR(datum, 'yyyymm') as dim_yearmonth_sheets_compatible,
            TO_CHAR(datum, 'iyyy/iw') as dim_yearcalendarweek,
            TO_CHAR(datum, 'iyyyiw') as dim_yearcalendarweek_sheets_compatible,
            TO_CHAR(datum, 'DY') as dim_dayofweek,
            EXTRACT(DOW FROM datum) as dim_dayofweek_num,
            EXTRACT(ISODOW FROM datum) as dim_isodayofweek_num,
            CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend' ELSE 'weekday' END as dim_weekend,
            date_trunc('month', datum)::date as dim_monthstart,
            (date_trunc('month', datum) + INTERVAL '1 MONTH' - INTERVAL '1 day')::date as dim_monthend
        FROM generate_series('2016-01-01'::date, '2026-12-31'::date, '1 day'::interval) AS sequence(datum);
    END IF;
END
$$;
