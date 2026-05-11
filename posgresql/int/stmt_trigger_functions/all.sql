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

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_1m_trigger ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_ins_trigger ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_upd_trigger ON timeframe_1m;

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

-- =========================================
-- timeframe_2m
-- =========================================
CREATE OR REPLACE FUNCTION update_timeframe_2m()
RETURNS TRIGGER AS $$
BEGIN

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
        FROM timeframe_2m
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
            FROM timeframe_2m
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
UPDATE timeframe_2m tf
SET sma10   = s.sma10,
    abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id
  AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_timeframe_2m_trigger     ON timeframe_2m;
DROP TRIGGER IF EXISTS update_timeframe_2m_ins_trigger ON timeframe_2m;
DROP TRIGGER IF EXISTS update_timeframe_2m_upd_trigger ON timeframe_2m;

CREATE TRIGGER update_timeframe_2m_ins_trigger
AFTER INSERT ON timeframe_2m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2m();

CREATE TRIGGER update_timeframe_2m_upd_trigger
AFTER UPDATE ON timeframe_2m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2m();


-- =========================================
-- timeframe_3m
-- =========================================
CREATE OR REPLACE FUNCTION update_timeframe_3m()
RETURNS TRIGGER AS $$
BEGIN

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
        FROM timeframe_3m
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
            FROM timeframe_3m
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
UPDATE timeframe_3m tf
SET sma10   = s.sma10,
    abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id
  AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_timeframe_3m_trigger     ON timeframe_3m;
DROP TRIGGER IF EXISTS update_timeframe_3m_ins_trigger ON timeframe_3m;
DROP TRIGGER IF EXISTS update_timeframe_3m_upd_trigger ON timeframe_3m;

CREATE TRIGGER update_timeframe_3m_ins_trigger
AFTER INSERT ON timeframe_3m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3m();

CREATE TRIGGER update_timeframe_3m_upd_trigger
AFTER UPDATE ON timeframe_3m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3m();

-- =========================================================================
-- Statement-level trigger conversions for timeframe_5m through timeframe_4h
--
-- Each trigger:
--   * uses a transition table (REFERENCING NEW TABLE AS new_rows) so the
--     websocket's batched UPSERT fires the trigger body only once per stmt
--   * guards against self-recursion with pg_trigger_depth() = 0, because the
--     internal UPDATE that writes sma10/abs_atr would otherwise re-fire the
--     UPDATE trigger
--   * separates INSERT and UPDATE triggers (PG requires separate triggers
--     when using transition tables on different event types)
-- =========================================================================


-- =========================================================================
-- timeframe_5m  ->  rolls up to 10m and 15m
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_5m()
RETURNS TRIGGER AS $$
BEGIN

-- SMA10 + ABS_ATR for affected rows
WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_5m
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_5m
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_5m tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

-- Rollup to 10m
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

-- Rollup to 15m
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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_5m_trigger     ON timeframe_5m;
DROP TRIGGER IF EXISTS update_timeframe_5m_ins_trigger ON timeframe_5m;
DROP TRIGGER IF EXISTS update_timeframe_5m_upd_trigger ON timeframe_5m;

CREATE TRIGGER update_timeframe_5m_ins_trigger
AFTER INSERT ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_5m();

CREATE TRIGGER update_timeframe_5m_upd_trigger
AFTER UPDATE ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_5m();


-- =========================================================================
-- timeframe_10m  ->  sma10/abs_atr only
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_10m()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_10m
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_10m
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_10m tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_10m_trigger     ON timeframe_10m;
DROP TRIGGER IF EXISTS update_timeframe_10m_ins_trigger ON timeframe_10m;
DROP TRIGGER IF EXISTS update_timeframe_10m_upd_trigger ON timeframe_10m;

CREATE TRIGGER update_timeframe_10m_ins_trigger
AFTER INSERT ON timeframe_10m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_10m();

CREATE TRIGGER update_timeframe_10m_upd_trigger
AFTER UPDATE ON timeframe_10m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_10m();


-- =========================================================================
-- timeframe_15m  ->  rolls up to 30m
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_15m()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_15m
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_15m
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_15m tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

-- Rollup to 30m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 30) AS bucket_start
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
    JOIN timeframe_15m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '30 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_30m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_15m_trigger     ON timeframe_15m;
DROP TRIGGER IF EXISTS update_timeframe_15m_ins_trigger ON timeframe_15m;
DROP TRIGGER IF EXISTS update_timeframe_15m_upd_trigger ON timeframe_15m;

CREATE TRIGGER update_timeframe_15m_ins_trigger
AFTER INSERT ON timeframe_15m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_15m();

CREATE TRIGGER update_timeframe_15m_upd_trigger
AFTER UPDATE ON timeframe_15m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_15m();


-- =========================================================================
-- timeframe_30m  ->  rolls up to 1h
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_30m()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_30m
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_30m
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_30m tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

-- Rollup to 1h (uses timeframe_min_start with 60, matching original)
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 60) AS bucket_start
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
    JOIN timeframe_30m t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '60 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_1h (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_30m_trigger     ON timeframe_30m;
DROP TRIGGER IF EXISTS update_timeframe_30m_ins_trigger ON timeframe_30m;
DROP TRIGGER IF EXISTS update_timeframe_30m_upd_trigger ON timeframe_30m;

CREATE TRIGGER update_timeframe_30m_ins_trigger
AFTER INSERT ON timeframe_30m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_30m();

CREATE TRIGGER update_timeframe_30m_upd_trigger
AFTER UPDATE ON timeframe_30m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_30m();


-- =========================================================================
-- timeframe_1h  ->  rolls up to 2h and 3h (uses timeframe_hour_start)
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_1h()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_1h
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_1h
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_1h tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

-- Rollup to 2h
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_hour_start(datetime, 120) AS bucket_start
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
    JOIN timeframe_1h t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '120 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_2h (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

-- Rollup to 3h
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_hour_start(datetime, 180) AS bucket_start
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
    JOIN timeframe_1h t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '180 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_3h (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_1h_trigger     ON timeframe_1h;
DROP TRIGGER IF EXISTS update_timeframe_1h_ins_trigger ON timeframe_1h;
DROP TRIGGER IF EXISTS update_timeframe_1h_upd_trigger ON timeframe_1h;

CREATE TRIGGER update_timeframe_1h_ins_trigger
AFTER INSERT ON timeframe_1h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_1h();

CREATE TRIGGER update_timeframe_1h_upd_trigger
AFTER UPDATE ON timeframe_1h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_1h();


-- =========================================================================
-- timeframe_2h  ->  rolls up to 4h
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_2h()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_2h
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_2h
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_2h tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

-- Rollup to 4h
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_hour_start(datetime, 240) AS bucket_start
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
    JOIN timeframe_2h t
      ON t.share_id = b.share_id
     AND t.datetime >= b.bucket_start
     AND t.datetime <  b.bucket_start + INTERVAL '240 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_4h (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_2h_trigger     ON timeframe_2h;
DROP TRIGGER IF EXISTS update_timeframe_2h_ins_trigger ON timeframe_2h;
DROP TRIGGER IF EXISTS update_timeframe_2h_upd_trigger ON timeframe_2h;

CREATE TRIGGER update_timeframe_2h_ins_trigger
AFTER INSERT ON timeframe_2h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2h();

CREATE TRIGGER update_timeframe_2h_upd_trigger
AFTER UPDATE ON timeframe_2h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2h();


-- =========================================================================
-- timeframe_3h  ->  sma10/abs_atr only
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_3h()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_3h
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_3h
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_3h tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_3h_trigger     ON timeframe_3h;
DROP TRIGGER IF EXISTS update_timeframe_3h_ins_trigger ON timeframe_3h;
DROP TRIGGER IF EXISTS update_timeframe_3h_upd_trigger ON timeframe_3h;

CREATE TRIGGER update_timeframe_3h_ins_trigger
AFTER INSERT ON timeframe_3h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3h();

CREATE TRIGGER update_timeframe_3h_upd_trigger
AFTER UPDATE ON timeframe_3h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3h();


-- =========================================================================
-- timeframe_4h  ->  sma10/abs_atr only
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_4h()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM timeframe_4h
        WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id, a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close FROM timeframe_4h
            WHERE share_id = a.share_id AND datetime <= a.datetime AND session = 1
            ORDER BY datetime DESC LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_4h tf
SET sma10 = s.sma10, abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_4h_trigger     ON timeframe_4h;
DROP TRIGGER IF EXISTS update_timeframe_4h_ins_trigger ON timeframe_4h;
DROP TRIGGER IF EXISTS update_timeframe_4h_upd_trigger ON timeframe_4h;

CREATE TRIGGER update_timeframe_4h_ins_trigger
AFTER INSERT ON timeframe_4h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_4h();

CREATE TRIGGER update_timeframe_4h_upd_trigger
AFTER UPDATE ON timeframe_4h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_4h();