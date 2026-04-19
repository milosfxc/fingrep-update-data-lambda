CREATE OR REPLACE FUNCTION update_timeframe_3m()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM timeframe_3m
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
    FROM timeframe_3m
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

UPDATE timeframe_3m SET
sma10 = _sma10,
abs_atr = _abs_atr
WHERE share_id = NEW.share_id AND
datetime = NEW.datetime;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timeframe_3m_trigger
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON timeframe_3m
FOR EACH ROW
EXECUTE FUNCTION update_timeframe_3m();