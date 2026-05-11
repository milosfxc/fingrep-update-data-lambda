CREATE OR REPLACE FUNCTION update_timeframe_1m()
RETURNS TRIGGER AS $$
BEGIN

-- Compute SMA10 and ABS_ATR for all affected rows in one pass, then update
WITH affected AS (
    SELECT DISTINCT share_id, datetime
    FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id,
        a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close
        FROM timeframe_1m
        WHERE share_id = a.share_id
          AND datetime <= a.datetime
          AND session = 1
        ORDER BY datetime DESC
        LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id,
        a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT
            high, low,
            LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close
            FROM timeframe_1m
            WHERE share_id = a.share_id
              AND datetime <= a.datetime
              AND session = 1
            ORDER BY datetime DESC
            LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_1m tf
SET sma10   = s.sma10,
    abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id
  AND tf.datetime = s.datetime;

-- Build the set of (share_id, bucket_start) pairs to rebuild for each timeframe
-- Done once and reused for 2m, 3m, 5m via a single CTE

-- TIMEFRAME 2m
WITH buckets AS (
    SELECT DISTINCT
        share_id,
        timeframe_min_start(datetime, 2) AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS datetime,
        (array_agg(t.open  ORDER BY t.datetime ASC))[1]  AS open,
        MAX(t.high)                                       AS high,
        MIN(t.low)                                        AS low,
        (array_agg(t.close ORDER BY t.datetime DESC))[1] AS close,
        SUM(t.volume)                                     AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
        (array_agg(t.session ORDER BY t.datetime ASC))[1] AS session,
        CAST(to_char(b.bucket_start, 'HH24MI') AS smallint) AS time
    FROM buckets b
    JOIN timeframe_1m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '2 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_2m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg
WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap,
    session = EXCLUDED.session,
    time    = EXCLUDED.time;

-- TIMEFRAME 3m
WITH buckets AS (
    SELECT DISTINCT
        share_id,
        timeframe_min_start(datetime, 3) AS bucket_start
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
    JOIN timeframe_1m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '3 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_3m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg
WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap,
    session = EXCLUDED.session,
    time    = EXCLUDED.time;

-- TIMEFRAME 5m
WITH buckets AS (
    SELECT DISTINCT
        share_id,
        timeframe_min_start(datetime, 5) AS bucket_start
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
    JOIN timeframe_1m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '5 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_5m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg
WHERE close IS NOT NULLDROP TRIGGER IF EXISTS update_timeframe_1m_trigger     ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_ins_trigger ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_upd_trigger ON timeframe_1m;
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap,
    session = EXCLUDED.session,
    time    = EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timeframe_1m_ins_trigger
AFTER INSERT ON timeframe_1m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_1m();

CREATE TRIGGER update_timeframe_1m_upd_trigger
AFTER UPDATE ON timeframe_1m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_1m();