CREATE OR REPLACE FUNCTION update_w_timeframe()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
	_avg_volume BIGINT;
	_rel_volume BIGINT;
    _convergence2 BOOLEAN;
	_convergence3 BOOLEAN;
	_month_start DATE;
	_month_end DATE;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM w_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	AND session = 1
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
	FROM w_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	AND session = 1
	ORDER BY date DESC
	LIMIT 14
)
SELECT
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END INTO _abs_atr
FROM
    last_14;

--AVG VOL
WITH last_20 AS (
	SELECT high, low, close, volume, vwap
	FROM w_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	AND session = 1
	ORDER BY date DESC
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
	SELECT date, high, low
	FROM w_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	AND session = 1
	ORDER BY date DESC
	LIMIT 2
)
SELECT
    CASE WHEN (SELECT COUNT(*) FROM last_2) = 2 AND
	MAX(high) = (SELECT high FROM last_2 ORDER BY date ASC LIMIT 1) AND
	MIN(low) = (SELECT low FROM last_2 ORDER BY date ASC LIMIT 1) THEN true ELSE false END AS _convergence2
	INTO _convergence2
FROM last_2;

--CONVERGENCE 3
WITH last_3 AS (
	SELECT date, high, low
	FROM w_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	AND session = 1
	ORDER BY date DESC
	LIMIT 3
)
SELECT
    CASE WHEN (SELECT COUNT(*) FROM last_3) = 3 AND
	MAX(high) = (SELECT high FROM last_3 ORDER BY date ASC LIMIT 1) AND
	MIN(low) = (SELECT low FROM last_3 ORDER BY date ASC LIMIT 1) THEN true ELSE false END AS _convergence3
	INTO _convergence3
FROM last_3;

UPDATE w_timeframe SET
sma10 = _sma10,
abs_atr = _abs_atr,
convergence2 = _convergence2,
convergence3 = _convergence3,
avg_volume = _avg_volume,
rel_volume = _rel_volume
WHERE share_id = NEW.share_id AND
date = NEW.date;

-- TIMEFRAME M CALCULATION
_month_start := date_trunc('month', NEW.date)::date;
_month_end := (_month_start + INTERVAL '1 month')::date;

INSERT INTO m_timeframe (
    share_id, date, open, high, low, close, volume, vwap
)
SELECT *
FROM (
    SELECT
        NEW.share_id AS share_id,
        _month_start AS date,
        (ARRAY_AGG(open ORDER BY date ASC))[1] AS open,
        MAX(high) AS high,
        MIN(low) AS low,
        (ARRAY_AGG(close ORDER BY date DESC))[1] AS close,
        SUM(volume) AS volume,
        CASE
            WHEN SUM(volume) > 0 THEN
                SUM(vwap * volume) / SUM(volume)
        END AS vwap
    FROM d_timeframe
    WHERE share_id = NEW.share_id
      AND date >= _month_start
      AND date < _month_end
) t
WHERE t.close IS NOT NULL
ON CONFLICT (share_id, date) DO UPDATE
SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap = EXCLUDED.vwap;


RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_w_timeframe_trigger
AFTER INSERT ON w_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_w_timeframe();