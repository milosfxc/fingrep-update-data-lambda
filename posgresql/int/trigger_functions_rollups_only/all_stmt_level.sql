-- =========================================================================
-- Statement-level trigger conversions — pure rollup triggers
--
-- Cascade chain (allowed to fire freely — no pg_trigger_depth guard):
--   timeframe_1m  -> 2m, 3m, 5m
--   timeframe_5m  -> 10m, 15m
--   timeframe_15m -> 30m
--   timeframe_30m -> 1h
--   timeframe_1h  -> 2h, 3h
--   timeframe_2h  -> 4h
--   w_timeframe   -> m_timeframe  (reads from d_timeframe)
--   m_timeframe   -> q_timeframe  (reads from d_timeframe)
-- =========================================================================


-- =========================================================================
-- timeframe_1m  ->  2m, 3m, 5m
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_1m()
RETURNS TRIGGER AS $$
BEGIN

-- 2m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 2) AS bucket_start
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
     AND t.datetime <  b.bucket_start + INTERVAL '2 minutes'
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO timeframe_2m (share_id, datetime, open, high, low, close, volume, vwap, session, time)
SELECT share_id, datetime, open, high, low, close, volume, vwap, session, time
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

-- 3m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 3) AS bucket_start
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
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

-- 5m
WITH buckets AS (
    SELECT DISTINCT share_id, timeframe_min_start(datetime, 5) AS bucket_start
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
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, datetime) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap, session=EXCLUDED.session, time=EXCLUDED.time;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_1m_trigger     ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_ins_trigger ON timeframe_1m;
DROP TRIGGER IF EXISTS update_timeframe_1m_upd_trigger ON timeframe_1m;

CREATE TRIGGER update_timeframe_1m_ins_trigger
AFTER INSERT ON timeframe_1m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_1m();

CREATE TRIGGER update_timeframe_1m_upd_trigger
AFTER UPDATE ON timeframe_1m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_1m();


-- =========================================================================
-- timeframe_5m  ->  10m, 15m
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_5m()
RETURNS TRIGGER AS $$
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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_timeframe_5m_trigger     ON timeframe_5m;
DROP TRIGGER IF EXISTS update_timeframe_5m_ins_trigger ON timeframe_5m;
DROP TRIGGER IF EXISTS update_timeframe_5m_upd_trigger ON timeframe_5m;

CREATE TRIGGER update_timeframe_5m_ins_trigger
AFTER INSERT ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_5m();

CREATE TRIGGER update_timeframe_5m_upd_trigger
AFTER UPDATE ON timeframe_5m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_5m();


-- =========================================================================
-- timeframe_15m  ->  30m
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_15m()
RETURNS TRIGGER AS $$
BEGIN

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
EXECUTE FUNCTION update_timeframe_15m();

CREATE TRIGGER update_timeframe_15m_upd_trigger
AFTER UPDATE ON timeframe_15m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_15m();


-- =========================================================================
-- timeframe_30m  ->  1h
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_30m()
RETURNS TRIGGER AS $$
BEGIN

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
EXECUTE FUNCTION update_timeframe_30m();

CREATE TRIGGER update_timeframe_30m_upd_trigger
AFTER UPDATE ON timeframe_30m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_30m();


-- =========================================================================
-- timeframe_1h  ->  2h, 3h
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_1h()
RETURNS TRIGGER AS $$
BEGIN

-- 2h
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

-- 3h
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
EXECUTE FUNCTION update_timeframe_1h();

CREATE TRIGGER update_timeframe_1h_upd_trigger
AFTER UPDATE ON timeframe_1h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_1h();


-- =========================================================================
-- timeframe_2h  ->  4h
-- =========================================================================
CREATE OR REPLACE FUNCTION update_timeframe_2h()
RETURNS TRIGGER AS $$
BEGIN

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
EXECUTE FUNCTION update_timeframe_2h();

CREATE TRIGGER update_timeframe_2h_upd_trigger
AFTER UPDATE ON timeframe_2h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_timeframe_2h();


-- =========================================================================
-- w_timeframe  ->  m_timeframe  (reads from d_timeframe, per original)
-- =========================================================================
CREATE OR REPLACE FUNCTION update_w_timeframe()
RETURNS TRIGGER AS $$
BEGIN

WITH buckets AS (
    SELECT DISTINCT
        share_id,
        date_trunc('month', date)::date AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS date,
        (array_agg(t.open  ORDER BY t.date ASC))[1]  AS open,
        MAX(t.high) AS high,
        MIN(t.low)  AS low,
        (array_agg(t.close ORDER BY t.date DESC))[1] AS close,
        SUM(t.volume) AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap
    FROM buckets b
    JOIN d_timeframe t
      ON t.share_id = b.share_id
     AND t.date >= b.bucket_start
     AND t.date <  (b.bucket_start + INTERVAL '1 month')::date
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO m_timeframe (share_id, date, open, high, low, close, volume, vwap)
SELECT share_id, date, open, high, low, close, volume, vwap
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, date) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_w_timeframe_trigger     ON w_timeframe;
DROP TRIGGER IF EXISTS update_w_timeframe_ins_trigger ON w_timeframe;
DROP TRIGGER IF EXISTS update_w_timeframe_upd_trigger ON w_timeframe;

CREATE TRIGGER update_w_timeframe_ins_trigger
AFTER INSERT ON w_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_w_timeframe();

CREATE TRIGGER update_w_timeframe_upd_trigger
AFTER UPDATE ON w_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_w_timeframe();


-- =========================================================================
-- m_timeframe  ->  q_timeframe  (reads from d_timeframe, per original)
-- =========================================================================
CREATE OR REPLACE FUNCTION update_m_timeframe()
RETURNS TRIGGER AS $$
BEGIN

WITH buckets AS (
    SELECT DISTINCT
        share_id,
        date_trunc('quarter', date)::date AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS date,
        (array_agg(t.open  ORDER BY t.date ASC))[1]  AS open,
        MAX(t.high) AS high,
        MIN(t.low)  AS low,
        (array_agg(t.close ORDER BY t.date DESC))[1] AS close,
        SUM(t.volume) AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap
    FROM buckets b
    JOIN d_timeframe t
      ON t.share_id = b.share_id
     AND t.date >= b.bucket_start
     AND t.date <  (b.bucket_start + INTERVAL '3 months')::date
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO q_timeframe (share_id, date, open, high, low, close, volume, vwap)
SELECT share_id, date, open, high, low, close, volume, vwap
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, date) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_m_timeframe_trigger     ON m_timeframe;
DROP TRIGGER IF EXISTS update_m_timeframe_ins_trigger ON m_timeframe;
DROP TRIGGER IF EXISTS update_m_timeframe_upd_trigger ON m_timeframe;

CREATE TRIGGER update_m_timeframe_ins_trigger
AFTER INSERT ON m_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_m_timeframe();

CREATE TRIGGER update_m_timeframe_upd_trigger
AFTER UPDATE ON m_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION update_m_timeframe();