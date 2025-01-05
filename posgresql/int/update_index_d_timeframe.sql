CREATE OR REPLACE FUNCTION update_indices_d_timeframe()
RETURNS TRIGGER AS $$
DECLARE
	_rel_change indices_d_timeframe.rel_change%type;
	_rel_gap indices_d_timeframe.rel_gap%type;
BEGIN
--CHANGE & GAP
WITH last_2 AS (
	SELECT date, close, open
	FROM indices_d_timeframe
	WHERE index_id = NEW.index_id
	AND date <= NEW.date
	ORDER BY date DESC LIMIT 2
)

SELECT ((close * 10000 / LAG(close, 1) OVER (ORDER BY date ASC)) - 10000) * 100 AS _rel_change,
((open * 10000 / LAG(close, 1) OVER (ORDER BY date ASC)) - 10000) * 100 AS _rel_gap
INTO _rel_change, _rel_gap FROM last_2 ORDER BY date DESC LIMIT 1;

--UPDATE
UPDATE indices_d_timeframe SET rel_change = _rel_change, rel_gap = _rel_gap
WHERE index_id = NEW.index_id AND date = NEW.date;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_indices_d_timeframe
AFTER INSERT ON indices_d_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_indices_d_timeframe();