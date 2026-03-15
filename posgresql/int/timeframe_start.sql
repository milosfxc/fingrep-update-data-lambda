CREATE OR REPLACE FUNCTION timeframe_min_start(ts TIMESTAMP, tf_minutes INTEGER)
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN date_trunc('hour', ts) +
           (floor(extract(minute FROM ts)::INTEGER / tf_minutes) * tf_minutes * INTERVAL '1 minute');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE OR REPLACE FUNCTION timeframe_hour_start(ts TIMESTAMP, tf_minutes INTEGER)
RETURNS TIMESTAMP
LANGUAGE SQL
IMMUTABLE
AS $$
SELECT
    to_timestamp(
        floor(
            extract(epoch FROM (ts - interval '1 hour'))
            / (tf_minutes * 60)
        ) * (tf_minutes * 60)
    ) + interval '1 hour';
$$;