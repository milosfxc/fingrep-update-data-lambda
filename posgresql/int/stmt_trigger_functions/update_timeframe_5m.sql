CREATE OR REPLACE FUNCTION public.update_timeframe_5m()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
-- 10m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 10) AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS datetime,
        (array_agg(t.open  ORDER BY t.datetime ASC))[1]  AS open,
        MAX(t.high) AS high,
        MIN(t.low)  AS low,
        (array_agg(t.close ORDER BY t.datetime DESC))[1] AS close,
        SUM(t.volume) AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
        (array_agg(t.session ORDER BY t.datetime ASC))[1] AS session,
        CAST(to_char(b.bucket_start, 'HH24MI') AS smallint) AS time
    FROM buckets b
    JOIN timeframe_5m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '10 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_10m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;
-- 15m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 15) AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS datetime,
        (array_agg(t.open  ORDER BY t.datetime ASC))[1]  AS open,
        MAX(t.high) AS high,
        MIN(t.low)  AS low,
        (array_agg(t.close ORDER BY t.datetime DESC))[1] AS close,
        SUM(t.volume) AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
        (array_agg(t.session ORDER BY t.datetime ASC))[1] AS session,
        CAST(to_char(b.bucket_start, 'HH24MI') AS smallint) AS time
    FROM buckets b
    JOIN timeframe_5m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '15 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_15m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$function$
-- Insert trigger
CREATE TRIGGER update_timeframe_5m_insert_trigger
AFTER INSERT
ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_5m();
-- Update trigger
CREATE TRIGGER update_timeframe_5m_update_trigger
AFTER UPDATE ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_5m();


