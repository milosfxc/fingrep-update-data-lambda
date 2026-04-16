CREATE OR REPLACE FUNCTION update_q_timeframe()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM q_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 10
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN AVG(close) END INTO _sma10 FROM last_10;

--ABS ATR
WITH last_14 AS (
	SELECT
	    high - low AS high_low,
        ABS(high - LAG(close) OVER (ORDER BY date)) AS high_prev_close,
        ABS(low - LAG(close) OVER (ORDER BY date)) AS low_prev_close
	FROM q_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 14
)
SELECT
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END INTO _abs_atr
FROM
    last_14;

UPDATE q_timeframe SET
sma10 = _sma10,
abs_atr = _abs_atr
WHERE share_id = NEW.share_id AND
date = NEW.date;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_q_timeframe_trigger
AFTER INSERT ON q_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_q_timeframe();