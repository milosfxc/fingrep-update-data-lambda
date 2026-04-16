CREATE OR REPLACE FUNCTION update_m_timeframe()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 BIGINT;
	_abs_atr BIGINT;
	_quarter_start DATE;
	_quarter_end DATE;
BEGIN

--SMA10
WITH last_10 AS (
	SELECT close
	FROM m_timeframe
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
	FROM m_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 14
)
SELECT
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END INTO _abs_atr
FROM
    last_14;

UPDATE m_timeframe SET
sma10 = _sma10,
abs_atr = _abs_atr
WHERE share_id = NEW.share_id AND
date = NEW.date;

-- TIMEFRAME Q CALCULATION
_quarter_start := date_trunc('quarter', NEW.date)::date;
_quarter_end   := (_quarter_start + INTERVAL '3 months')::date;

INSERT INTO q_timeframe (
    share_id, date, open, high, low, close, volume, vwap
)
SELECT *
FROM (
    SELECT
        NEW.share_id AS share_id,
        _quarter_start AS date,
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
      AND date >= _quarter_start
      AND date < _quarter_end
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

CREATE TRIGGER update_m_timeframe_trigger
AFTER INSERT ON m_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_m_timeframe();