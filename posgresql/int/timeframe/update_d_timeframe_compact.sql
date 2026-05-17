CREATE OR REPLACE FUNCTION update_d_timeframe_compact()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
	_abs_change d_timeframe.abs_change%type;
	_rel_change d_timeframe.rel_change%type;
	_rel_gap d_timeframe.rel_gap%type;
	_rel_change_from_open d_timeframe.rel_change_from_open%type;
	_week_start DATE;
    _week_end DATE;
BEGIN
--CHANGES FROM PREVIOUS DAY
WITH prev_1 AS (
	SELECT close, open
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date < NEW.date
	ORDER BY date DESC LIMIT 1
)
SELECT NEW.close - p_1.close AS _abs_change,
((NEW.close * _magn / NULLIF(p_1.close,0)) - 10000) * 100 AS _rel_change,
((NEW.open * _magn / NULLIF(p_1.close,0)) - 10000) * 100 AS _rel_gap
INTO _abs_change, _rel_change, _rel_gap FROM prev_1 p_1;

--AFTER HOURS CHANGE
IF _rel_change IS NOT NULL AND _rel_gap IS NOT NULL THEN
	_rel_change_from_open := _rel_change - _rel_gap;
END IF;

--UPDATE
UPDATE d_timeframe SET
abs_change = _abs_change, rel_change = _rel_change, rel_gap = _rel_gap, rel_change_from_open = _rel_change_from_open
WHERE share_id = NEW.share_id AND date = NEW.date;

-- TIMEFRAME W CALCULATION
_week_start := date_trunc('week', NEW.date)::date;
_week_end   := (_week_start + INTERVAL '1 week')::date;

INSERT INTO w_timeframe (
    share_id,
    date,
    open,
    high,
    low,
    close,
    volume,
    vwap
)
SELECT * FROM (
    SELECT
        NEW.share_id AS share_id,
        _week_start AS date,
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
      AND date >= _week_start
      AND date < _week_end
) t WHERE t.close IS NOT NULL
ON CONFLICT (share_id, date) DO UPDATE
SET
    open    = EXCLUDED.open,
    high    = EXCLUDED.high,
    low     = EXCLUDED.low,
    close   = EXCLUDED.close,
    volume  = EXCLUDED.volume,
    vwap    = EXCLUDED.vwap;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_d_timeframe_compact_trigger
AFTER INSERT OR UPDATE OF open, high, low, close, volume
ON d_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_d_timeframe_compact();