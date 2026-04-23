CREATE OR REPLACE FUNCTION update_timeframe_1m_bulk()
RETURNS TRIGGER AS $$
DECLARE
    _magn BIGINT := 10000;
BEGIN

-- SMA10 + ABS ATR: update for all affected rows
WITH affected AS (
    SELECT DISTINCT share_id, datetime
    FROM inserted_rows
),
last_10_base AS (
    SELECT
        a.share_id,
        a.datetime AS ref_datetime,
        t.close,
        ROW_NUMBER() OVER (PARTITION BY a.share_id, a.datetime ORDER BY t.datetime DESC) AS rn
    FROM affected a
    JOIN timeframe_1m t
        ON t.share_id = a.share_id
        AND t.datetime <= a.datetime
        AND t.session = 1
),
sma10_calc AS (
    SELECT
        share_id,
        ref_datetime,
        CASE WHEN COUNT(*) = 10 THEN AVG(close) END AS sma10
    FROM last_10_base
    WHERE rn <= 10
    GROUP BY share_id, ref_datetime
),
last_15_base AS (
    SELECT
        a.share_id,
        a.datetime AS ref_datetime,
        t.datetime AS bar_datetime,
        t.high,
        t.low,
        t.close,
        ROW_NUMBER() OVER (PARTITION BY a.share_id, a.datetime ORDER BY t.datetime DESC) AS rn
    FROM affected a
    JOIN timeframe_1m t
        ON t.share_id = a.share_id
        AND t.datetime <= a.datetime
        AND t.session = 1
),
last_15_filtered AS (
    SELECT *
    FROM last_15_base
    WHERE rn <= 15
),
tr_calc AS (
    SELECT
        share_id,
        ref_datetime,
        high - low AS high_low,
        ABS(high - LAG(close) OVER (PARTITION BY share_id, ref_datetime ORDER BY bar_datetime)) AS high_prev_close,
        ABS(low  - LAG(close) OVER (PARTITION BY share_id, ref_datetime ORDER BY bar_datetime)) AS low_prev_close
    FROM last_15_filtered
),
atr_calc AS (
    SELECT
        share_id,
        ref_datetime,
        CASE WHEN COUNT(*) = 14 THEN
            AVG(GREATEST(high_low, high_prev_close, low_prev_close))
        END AS abs_atr
    FROM tr_calc
    WHERE high_prev_close IS NOT NULL
    GROUP BY share_id, ref_datetime
)
UPDATE timeframe_1m t
SET
    sma10   = s.sma10,
    abs_atr = r.abs_atr
FROM affected a
LEFT JOIN sma10_calc s ON s.share_id = a.share_id AND s.ref_datetime = a.datetime
LEFT JOIN atr_calc   r ON r.share_id = a.share_id AND r.ref_datetime = a.datetime
WHERE t.share_id = a.share_id
  AND t.datetime = a.datetime;

-- TIMEFRAME 2m
INSERT INTO timeframe_2m (
    share_id, datetime, open, high, low, close, volume, vwap, session, time
)
SELECT
    t.share_id,
    timeframe_min_start(t.datetime, 2) AS datetime,
    (ARRAY_AGG(t.open    ORDER BY t.datetime ASC))[1]  AS open,
    MAX(t.high)                                         AS high,
    MIN(t.low)                                          AS low,
    (ARRAY_AGG(t.close   ORDER BY t.datetime DESC))[1] AS close,
    SUM(t.volume)                                       AS volume,
    CASE WHEN SUM(t.volume) > 0 THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
    (ARRAY_AGG(t.session ORDER BY t.datetime ASC))[1]  AS session,
    CAST(to_char(timeframe_min_start(t.datetime, 2), 'HH24MI') AS smallint) AS time
FROM timeframe_1m t
WHERE (t.share_id, timeframe_min_start(t.datetime, 2)) IN (
    SELECT share_id, timeframe_min_start(datetime, 2)
    FROM inserted_rows
)
GROUP BY t.share_id, timeframe_min_start(t.datetime, 2)
HAVING (ARRAY_AGG(t.close ORDER BY t.datetime DESC))[1] IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE
SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap,
    session = EXCLUDED.session,
    time    = EXCLUDED.time;

-- TIMEFRAME 3m
INSERT INTO timeframe_3m (
    share_id, datetime, open, high, low, close, volume, vwap, session, time
)
SELECT
    t.share_id,
    timeframe_min_start(t.datetime, 3) AS datetime,
    (ARRAY_AGG(t.open    ORDER BY t.datetime ASC))[1]  AS open,
    MAX(t.high)                                         AS high,
    MIN(t.low)                                          AS low,
    (ARRAY_AGG(t.close   ORDER BY t.datetime DESC))[1] AS close,
    SUM(t.volume)                                       AS volume,
    CASE WHEN SUM(t.volume) > 0 THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
    (ARRAY_AGG(t.session ORDER BY t.datetime ASC))[1]  AS session,
    CAST(to_char(timeframe_min_start(t.datetime, 3), 'HH24MI') AS smallint) AS time
FROM timeframe_1m t
WHERE (t.share_id, timeframe_min_start(t.datetime, 3)) IN (
    SELECT share_id, timeframe_min_start(datetime, 3)
    FROM inserted_rows
)
GROUP BY t.share_id, timeframe_min_start(t.datetime, 3)
HAVING (ARRAY_AGG(t.close ORDER BY t.datetime DESC))[1] IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE
SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap,
    session = EXCLUDED.session,
    time    = EXCLUDED.time;

-- TIMEFRAME 5m
INSERT INTO timeframe_5m (
    share_id, datetime, open, high, low, close, volume, vwap, session, time
)
SELECT
    t.share_id,
    timeframe_min_start(t.datetime, 5) AS datetime,
    (ARRAY_AGG(t.open    ORDER BY t.datetime ASC))[1]  AS open,
    MAX(t.high)                                         AS high,
    MIN(t.low)                                          AS low,
    (ARRAY_AGG(t.close   ORDER BY t.datetime DESC))[1] AS close,
    SUM(t.volume)                                       AS volume,
    CASE WHEN SUM(t.volume) > 0 THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap,
    (ARRAY_AGG(t.session ORDER BY t.datetime ASC))[1]  AS session,
    CAST(to_char(timeframe_min_start(t.datetime, 5), 'HH24MI') AS smallint) AS time
FROM timeframe_1m t
WHERE (t.share_id, timeframe_min_start(t.datetime, 5)) IN (
    SELECT share_id, timeframe_min_start(datetime, 5)
    FROM inserted_rows
)
GROUP BY t.share_id, timeframe_min_start(t.datetime, 5)
HAVING (ARRAY_AGG(t.close ORDER BY t.datetime DESC))[1] IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE
SET
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

CREATE TRIGGER update_timeframe_1m_trigger_bulk
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON timeframe_1m
REFERENCING NEW TABLE AS inserted_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_1m_bulk();