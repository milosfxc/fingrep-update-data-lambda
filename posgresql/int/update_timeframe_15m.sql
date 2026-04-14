CREATE OR REPLACE FUNCTION update_timeframe_15m()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
	_timeframe_30m_start TIMESTAMP;
	_timeframe_30m_end TIMESTAMP;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM timeframe_15m
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
    FROM timeframe_15m
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

-- UPDATE CURRENT TABLE
UPDATE timeframe_15m SET
sma10 = _sma10,
abs_atr = _abs_atr
WHERE share_id = NEW.share_id AND
datetime = NEW.datetime;

-- TIMEFRAME 30M CALCULATION
_timeframe_30m_start := timeframe_min_start(NEW.datetime, 30);
_timeframe_30m_end := _timeframe_30m_start + INTERVAL '30 minutes';

INSERT INTO timeframe_30m (
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
        _timeframe_30m_start AS datetime,
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
        CAST(to_char(_timeframe_30m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_15m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_30m_start
      AND datetime < _timeframe_30m_end
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

CREATE TRIGGER update_timeframe_15m_trigger
AFTER INSERT ON timeframe_15m
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_15m();