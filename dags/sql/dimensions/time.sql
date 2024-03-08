{% if is_modified %}
DROP TABLE IF EXISTS {{ schema}}.dim__time;
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
            id DATE PRIMARY KEY,
            date DATE,
            year INT,
            month INT,
            dayofmonth INT,
            dayofyear INT,
            calendarweek INT,
            formatteddate TEXT,
            quartal TEXT,
            yearquartal TEXT,
            yearmonth TEXT,
            yearmonth_sheets_compatible TEXT,
            yearcalendarweek TEXT,
            yearcalendarweek_sheets_compatible TEXT,
            dayofweek TEXT,
            dayofweek_num INT,
            isodayofweek_num INT,
            weekend TEXT,
            monthstart DATE,
            monthend DATE
        );

        -- Set the flag since the table was just created
        table_was_created := TRUE;

        -- Create an index on the id column if the table was just created
        CREATE INDEX IF NOT EXISTS idx_dim__time_id ON public.dim__time(id);
    END IF;

    -- If the table was just created, perform the INSERT operation
    IF table_was_created THEN
        INSERT INTO dim__time (
            id,
            date,
            year,
            month,
            dayofmonth,
            dayofyear,
            calendarweek,
            formatteddate,
            quartal,
            yearquartal,
            yearmonth,
            yearmonth_sheets_compatible,
            yearcalendarweek,
            yearcalendarweek_sheets_compatible,
            dayofweek,
            dayofweek_num,
            isodayofweek_num,
            weekend,
            monthstart,
            monthend
        )
        SELECT
            datum as id,
            datum as date,
            EXTRACT(YEAR FROM datum) as year,
            EXTRACT(MONTH FROM datum) as month,
            EXTRACT(DAY FROM datum) as dayofmonth,
            EXTRACT(DOY FROM datum) as dayofyear,
            EXTRACT(WEEK FROM datum) as calendarweek,
            TO_CHAR(datum, 'yyyy-mm-dd') as formatteddate,
            'q' || TO_CHAR(datum, 'q') as quartal,
            TO_CHAR(datum, 'yyyy/"q"q') as yearquartal,
            TO_CHAR(datum, 'yyyy/mm') as yearmonth,
            TO_CHAR(datum, 'yyyymm') as yearmonth_sheets_compatible,
            TO_CHAR(datum, 'iyyy/iw') as yearcalendarweek,
            TO_CHAR(datum, 'iyyyiw') as yearcalendarweek_sheets_compatible,
            TO_CHAR(datum, 'DY') as dayofweek,
            EXTRACT(DOW FROM datum) as dayofweek_num,
            EXTRACT(ISODOW FROM datum) as isodayofweek_num,
            CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend' ELSE 'weekday' END as weekend,
            datum + (1 - EXTRACT(DAY FROM datum))::integer as monthstart,
            (datum + INTERVAL '1 month' - INTERVAL '1 day')::date as monthend
        FROM generate_series('2016-01-01'::date, '2026-12-31'::date, '1 day'::interval) AS sequence(datum);
    END IF;
END
$$;
