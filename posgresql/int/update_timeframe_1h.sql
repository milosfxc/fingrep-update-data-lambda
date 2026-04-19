CREATE OR REPLACE FUNCTION update_timeframe_1h()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
	_timeframe_2h_start TIMESTAMP;
	_timeframe_2h_end TIMESTAMP;
    _timeframe_3h_start TIMESTAMP;
	_timeframe_3h_end TIMESTAMP;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM timeframe_1h
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
	AND session = 1
	ORDER BY datetime DESC
	LIMIT 10
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN AVG(close) END INTO _sma10 FROM last_10;

--ABS ATR
WITH last_15 AS (
    SELECT datetime, high, low, close
    FROM timeframe_1h
    WHERE share_id = NEW.share_id
      AND datetime <= NEW.datetime
      AND session = 1
    ORDER BY datetime DESC
    LIMIT 15
),
ordered AS (
    SELECT *
    FROM last_15
    ORDER BY datetime ASC
),
tr_values AS (
    SELECT
        high - low AS high_low,
        ABS(high - LAG(close) OVER (ORDER BY datetime)) AS high_prev_close,
        ABS(low  - LAG(close) OVER (ORDER BY datetime)) AS low_prev_close
    FROM ordered
)
SELECT
    CASE
        WHEN COUNT(*) = 14 THEN
            AVG(GREATEST(high_low, high_prev_close, low_prev_close))
    END
INTO _abs_atr
FROM tr_values
WHERE high_prev_close IS NOT NULL;

UPDATE timeframe_1h SET
sma10 = _sma10,
abs_atr = _abs_atr
WHERE share_id = NEW.share_id AND
datetime = NEW.datetime;

-- TIMEFRAME 2h CALCULATION
_timeframe_2h_start := timeframe_hour_start(NEW.datetime, 120);
_timeframe_2h_end := _timeframe_2h_start + INTERVAL '120 minutes';

INSERT INTO timeframe_2h (
    share_id,
    datetime,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    session,
    time
)
SELECT * FROM (
    SELECT
        NEW.share_id AS share_id,
        _timeframe_2h_start AS datetime,
        (ARRAY_AGG(open ORDER BY datetime ASC))[1] AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        (ARRAY_AGG(close ORDER BY datetime DESC))[1] AS close,
        SUM(volume) AS volume,
        CASE
            WHEN SUM(volume) > 0 THEN
                SUM(vwap * volume) / SUM(volume)
        END AS vwap,
        (ARRAY_AGG(session ORDER BY datetime ASC))[1] AS session,
        CAST(to_char(_timeframe_2h_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_1h
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_2h_start
      AND datetime < _timeframe_2h_end
) t
WHERE t.close IS NOT NULL
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

-- TIMEFRAME 3h CALCULATION
_timeframe_3h_start := timeframe_hour_start(NEW.datetime, 180);
_timeframe_3h_end := _timeframe_3h_start + INTERVAL '180 minutes';

INSERT INTO timeframe_3h (
    share_id,
    datetime,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    session,
    time
)
SELECT * FROM (
    SELECT
        NEW.share_id AS share_id,
        _timeframe_3h_start AS datetime,
        (ARRAY_AGG(open ORDER BY datetime ASC))[1] AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        (ARRAY_AGG(close ORDER BY datetime DESC))[1] AS close,
        SUM(volume) AS volume,
        CASE
            WHEN SUM(volume) > 0 THEN
                SUM(vwap * volume) / SUM(volume)
        END AS vwap,
        (ARRAY_AGG(session ORDER BY datetime ASC))[1] AS session,
        CAST(to_char(_timeframe_3h_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_1h
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_3h_start
      AND datetime < _timeframe_3h_end
) t
WHERE t.close IS NOT NULL
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

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timeframe_1h_trigger
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON timeframe_1h
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_1h();