CREATE OR REPLACE FUNCTION update_timeframe_5m()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
	_avg_volume BIGINT;
	_rel_volume BIGINT;
    _convergence2 BOOLEAN;
	_convergence3 BOOLEAN;
	_timeframe_15m_start TIMESTAMP;
	_timeframe_15m_end TIMESTAMP;
	_open BIGINT;
	_close BIGINT;
	_high BIGINT;
	_low BIGINT;
	_volume BIGINT;
	_vwap BIGINT;
	_session SMALLINT;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM timeframe_5m
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
    AND session = 1
	ORDER BY datetime DESC
	LIMIT 10
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN AVG(close) END INTO _sma10 FROM last_10;

--ABS ATR
WITH last_14 AS (
	SELECT
	    high - low AS high_low,
        ABS(high - LAG(close) OVER (ORDER BY datetime)) AS high_prev_close,
        ABS(low - LAG(close) OVER (ORDER BY datetime)) AS low_prev_close
	FROM timeframe_5m
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
    AND session = 1
	ORDER BY datetime DESC
	LIMIT 14
)
SELECT
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END INTO _abs_atr
FROM
    last_14;

--AVG VOL
WITH last_20 AS (
	SELECT high, low, close, volume, vwap
	FROM timeframe_5m
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
	AND session = 1
	ORDER BY datetime DESC
	LIMIT 20
)
SELECT
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN AVG(volume) END AS _avg_volume
INTO
	_avg_volume
FROM last_20;

--RVOL
IF _avg_volume > 0 AND NEW.volume > 0 THEN
    _rel_volume := NEW.volume * _magn / _avg_volume;
END IF;

--CONVERGENCE 2
WITH last_2 AS (
	SELECT datetime, high, low
	FROM timeframe_5m
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
	AND session = 1
	ORDER BY datetime DESC
	LIMIT 2
)
SELECT
    CASE WHEN (SELECT COUNT(*) FROM last_2) = 2 AND
	MAX(high) = (SELECT high FROM last_2 ORDER BY datetime ASC LIMIT 1) AND
	MIN(low) = (SELECT low FROM last_2 ORDER BY datetime ASC LIMIT 1) THEN true ELSE false END AS _convergence2
	INTO _convergence2
FROM last_2;

--CONVERGENCE 3
WITH last_3 AS (
	SELECT datetime, high, low
	FROM timeframe_5m
	WHERE share_id = NEW.share_id
	AND datetime <= NEW.datetime
	AND session = 1
	ORDER BY datetime DESC
	LIMIT 3
)
SELECT
    CASE WHEN (SELECT COUNT(*) FROM last_3) = 3 AND
	MAX(high) = (SELECT high FROM last_3 ORDER BY datetime ASC LIMIT 1) AND
	MIN(low) = (SELECT low FROM last_3 ORDER BY datetime ASC LIMIT 1) THEN true ELSE false END AS _convergence3
	INTO _convergence3
FROM last_3;

UPDATE timeframe_5m SET
sma10 = _sma10,
abs_atr = _abs_atr,
convergence2 = _convergence2,
convergence3 = _convergence3,
avg_volume = _avg_volume,
rel_volume = _rel_volume
WHERE share_id = NEW.share_id AND
datetime = NEW.datetime;

--TIMEFRAME 10m
_timeframe_10m_start := timeframe_min_start(NEW.datetime, 10);
_timeframe_10m_end := _timeframe_10m_start + INTERVAL '10 minutes';

SELECT
    (ARRAY_AGG(open ORDER BY datetime ASC))[1],
    MAX(high),
    MIN(low),
    (ARRAY_AGG(close ORDER BY datetime DESC))[1],
    SUM(volume),
    CASE
        WHEN SUM(volume) > 0 THEN
            SUM(vwap * volume) / SUM(volume)
    END,
    (ARRAY_AGG(session ORDER BY datetime ASC))[1]
    INTO
    _open,_high,_low,_close,_volume,_vwap,_session
FROM timeframe_5m
WHERE share_id = NEW.share_id
  AND datetime >= _timeframe_10m_start
  AND datetime < _timeframe_10m_end;

IF _close > 0 AND _session IS NOT NULL THEN
    INSERT INTO timeframe_10m (
        share_id,
        datetime,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        session
    )
    VALUES (NEW.share_id, _timeframe_10m_start, _open, _high, _low, _close, _volume, _vwap, _session)
    ON CONFLICT (share_id, datetime) DO UPDATE
    SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume,
        vwap   = EXCLUDED.vwap,
        session = EXCLUDED.session;
END IF;

--TIMEFRAME 15m
_timeframe_15m_start := timeframe_min_start(NEW.datetime, 15);
_timeframe_15m_end := _timeframe_15m_start + INTERVAL '15 minutes';

SELECT
    (ARRAY_AGG(open ORDER BY datetime ASC))[1],
    MAX(high),
    MIN(low),
    (ARRAY_AGG(close ORDER BY datetime DESC))[1],
    SUM(volume),
    CASE
        WHEN SUM(volume) > 0 THEN
            SUM(vwap * volume) / SUM(volume)
    END,
    (ARRAY_AGG(session ORDER BY datetime ASC))[1]
    INTO
    _open,_high,_low,_close,_volume,_vwap,_session
FROM timeframe_5m
WHERE share_id = NEW.share_id
  AND datetime >= _timeframe_15m_start
  AND datetime < _timeframe_15m_end;

IF _close > 0 AND _session IS NOT NULL THEN
    INSERT INTO timeframe_15m (
        share_id,
        datetime,
        open,
        high,
        low,
        close,
        volume,
        vwap,
        session
    )
    VALUES (NEW.share_id, _timeframe_15m_start, _open, _high, _low, _close, _volume, _vwap, _session)
    ON CONFLICT (share_id, datetime) DO UPDATE
    SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume,
        vwap   = EXCLUDED.vwap,
        session = EXCLUDED.session;
END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timeframe_5m_trigger
AFTER INSERT ON timeframe_5m
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_5m();