CREATE OR REPLACE FUNCTION update_timeframe_5m()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _timeframe_10m_start TIMESTAMP;
	_timeframe_10m_end TIMESTAMP;
	_timeframe_15m_start TIMESTAMP;
	_timeframe_15m_end TIMESTAMP;
BEGIN

-- TIMEFRAME 10m CALCULATION
_timeframe_10m_start := timeframe_min_start(NEW.datetime, 10);
_timeframe_10m_end := _timeframe_10m_start + INTERVAL '10 minutes';

INSERT INTO timeframe_10m (
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
        _timeframe_10m_start AS datetime,
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
        CAST(to_char(_timeframe_10m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_5m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_10m_start
      AND datetime < _timeframe_10m_end
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

-- TIMEFRAME 15m CALCULATION
_timeframe_15m_start := timeframe_min_start(NEW.datetime, 15);
_timeframe_15m_end := _timeframe_15m_start + INTERVAL '15 minutes';

INSERT INTO timeframe_15m (
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
        _timeframe_15m_start AS datetime,
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
        CAST(to_char(_timeframe_15m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_5m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_15m_start
      AND datetime < _timeframe_15m_end
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

CREATE TRIGGER update_timeframe_5m_trigger
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON timeframe_5m
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_5m();