CREATE OR REPLACE FUNCTION update_timeframe_1m()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _timeframe_2m_start TIMESTAMP;
	_timeframe_2m_end TIMESTAMP;
	_timeframe_3m_start TIMESTAMP;
	_timeframe_3m_end TIMESTAMP;
    _timeframe_5m_start TIMESTAMP;
	_timeframe_5m_end TIMESTAMP;
BEGIN

-- TIMEFRAME 2m CALCULATION
_timeframe_2m_start := timeframe_min_start(NEW.datetime, 2);
_timeframe_2m_end := _timeframe_2m_start + INTERVAL '2 minutes';

INSERT INTO timeframe_2m (
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
        _timeframe_2m_start AS datetime,
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
        CAST(to_char(_timeframe_2m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_1m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_2m_start
      AND datetime < _timeframe_2m_end
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

-- TIMEFRAME 3m CALCULATION
_timeframe_3m_start := timeframe_min_start(NEW.datetime, 3);
_timeframe_3m_end := _timeframe_3m_start + INTERVAL '3 minutes';

INSERT INTO timeframe_3m (
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
        _timeframe_3m_start AS datetime,
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
        CAST(to_char(_timeframe_3m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_1m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_3m_start
      AND datetime < _timeframe_3m_end
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

-- TIMEFRAME 5m CALCULATION
_timeframe_5m_start := timeframe_min_start(NEW.datetime, 5);
_timeframe_5m_end := _timeframe_5m_start + INTERVAL '5 minutes';

INSERT INTO timeframe_5m (
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
        _timeframe_5m_start AS datetime,
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
        CAST(to_char(_timeframe_5m_start, 'HH24MI') AS smallint) AS time
    FROM timeframe_1m
    WHERE share_id = NEW.share_id
      AND datetime >= _timeframe_5m_start
      AND datetime < _timeframe_5m_end
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

CREATE TRIGGER update_timeframe_1m_trigger
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON timeframe_1m
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_1m();