CREATE OR REPLACE FUNCTION timeframe_min_start(ts TIMESTAMP, tf_minutes INTEGER)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN date_trunc('hour', ts) +
           (floor(extract(minute FROM ts)::INTEGER / tf_minutes) * tf_minutes * INTERVAL '1 minute');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION timeframe_hour_start(ts TIMESTAMP, tf_minutes INTEGER)
RETURNS TIMESTAMP AS $$
SELECT
    date_trunc('hour', ts)
    - (extract(hour FROM ts)::int % (tf_minutes / 60)) * interval '1 hour'; -- tf_minutes is always divisible by 60,
$$ LANGUAGE SQL IMMUTABLE;                                                  -- so this can't work for half-hour timeframes