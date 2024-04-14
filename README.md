# fingrep-update-data-lambda
* Check for new IPOs
* Check for ticker changes 
* Check for splits

# initial setup
* Get historical prices
* Get company info data
* Get historical economic calendar
* Get historical split data

https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apiKey=KAKTnsmvIxPYvpwuzancIju96yzwiU5U
https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apikey=KAKTnsmvIxPYvpwuzancIju96yzwiU5U


# finviz2yahoo mapping for sectors and industries
* Financial -> Financial Services (sector)
* None -> Exchange Traded Fund (industry)
* Closed-End Fund - Equity -> Asset Management (industry)
* Closed-End Fund - Foreign -> Asset Management (industry)
* Closed-End Fund - Debt -> Asset Management (industry)

# trigger function for database
CREATE OR REPLACE FUNCTION update_d_timeframe() 
RETURNS TRIGGER AS $$
DECLARE 
    _sma10 d_timeframe.sma10%type;
    _sma20 d_timeframe.sma20%type;
    _sma50 d_timeframe.sma50%type;
    _sma100 d_timeframe.sma100%type;
    _sma200 d_timeframe.sma200%type;
	_rel_atr d_timeframe.rel_atr%type;
	_abs_atr d_timeframe.abs_atr%type;
	_rel_adr d_timeframe.rel_adr%type;
    _abs_adr d_timeframe.abs_adr%type;
	_avg_volume d_timeframe.avg_volume%type;
	_rel_volume d_timeframe.rel_volume%type;
	_abs_change d_timeframe.abs_change%type;
	_rel_change d_timeframe.rel_change%type;
	_rel_gap d_timeframe.rel_gap%type;
	_dense_volume d_timeframe.dense_volume%type;
	_avg_volume_40 NUMERIC;
	_avg_volume_ytd NUMERIC;
	_convergence d_timeframe.convergence%type;
BEGIN
--SMA10
WITH last_10 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 10
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN ROUND(AVG(close), 4) END INTO _sma10 FROM last_10;
	
--ABS_ATR & REL_ATR
WITH last_14 AS (
	SELECT 
	    high - low AS high_low,
        ABS(high - LAG(close) OVER (ORDER BY date)) AS high_prev_close,
        ABS(low - LAG(close) OVER (ORDER BY date)) AS low_prev_close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY Date DESC
	LIMIT 14
)
SELECT 
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN ROUND(AVG(GREATEST(high_low, high_prev_close, low_prev_close)),4) END INTO _abs_atr
FROM 
    last_14;
	
SELECT CASE WHEN _abs_atr IS NOT NULL AND NEW.close <> 0 THEN ROUND(_abs_atr/NEW.close*100,2) END INTO _rel_atr;

--SMA20 & ADR & AVGVOL 
WITH last_20 AS (
	SELECT high, low, close, volume
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 20
)
SELECT 
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(100 * (AVG(high/low) - 1), 2) END AS _rel_adr,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND((AVG(high - low)), 2) END AS _abs_adr,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(AVG(close), 4) END AS _sma20,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(AVG(volume)) ELSE 0 END AS _avg_volume
INTO 
	_rel_adr, _abs_adr, _sma20, _avg_volume
FROM last_20;
--DENSE VOLUME
WITH last_40 AS (
	SELECT volume
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 40
)
SELECT 
	CASE WHEN (SELECT COUNT(*) FROM last_40) = 40 THEN ROUND(AVG(volume)) ELSE 0 END AS _avg_volume_40
INTO _avg_volume_40
FROM last_40;

WITH last_252 AS (
	SELECT volume FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 252
)
SELECT 
	CASE WHEN (SELECT COUNT(*) FROM last_252) = 252 THEN ROUND(AVG(volume)) ELSE 0 END AS _avg_volume_ytd
INTO _avg_volume_ytd
FROM last_252;

SELECT 
	CASE WHEN _avg_volume_40 != 0 AND _avg_volume_ytd != 0 THEN ROUND(_avg_volume_40/_avg_volume_ytd, 2) ELSE 0 END AS _dense_volume
INTO _dense_volume;
--RVOL
SELECT CASE WHEN _avg_volume != 0 THEN ROUND(volume::numeric/_avg_volume, 2) END INTO _rel_volume 
FROM d_timeframe WHERE share_id = NEW.share_id AND date = NEW.date;
--SMA50
WITH last_50 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 50
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_50) = 50 THEN ROUND(AVG(close), 4) END INTO _sma50 FROM last_50;

--SMA100
WITH last_100 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 100
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_100) = 100 THEN ROUND(AVG(close), 4) END INTO _sma100 FROM last_100;

--SMA200
WITH last_200 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 200
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_200) = 200 THEN ROUND(AVG(close), 4) END INTO _sma200 FROM last_200;

--CHANGE FROM PREVIOUS DAY
WITH last_2 AS (
	SELECT date, close, open 
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC LIMIT 2
)
SELECT close - LAG(close, 1) OVER (ORDER BY date ASC) AS _abs_change, 
ROUND(((close/ LAG(close, 1) OVER (ORDER BY date ASC)) - 1) * 100, 2) AS _rel_change,
ROUND(((open/ LAG(close, 1) OVER (ORDER BY date ASC)) - 1) * 100, 2) AS _rel_gap 
INTO _abs_change, _rel_change, _rel_gap FROM last_2 ORDER BY date DESC LIMIT 1;

--CONVERGENCE
WITH last_3 AS (
	SELECT date, high, low 
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 3
)
SELECT 
    CASE WHEN (SELECT COUNT(*) FROM last_3) = 3 AND
	MAX(high) = (SELECT high FROM last_3 ORDER BY date ASC LIMIT 1) AND 
	MIN(low) = (SELECT low FROM last_3 ORDER BY date ASC LIMIT 1) THEN true ELSE false END AS _convergence
	INTO _convergence
FROM last_3;
	
--UPDATE
UPDATE d_timeframe SET sma10 = _sma10, sma20 = _sma20, sma50 = _sma50, sma100 = _sma100, sma200 = _sma200, 
abs_atr = _abs_atr, rel_atr = _rel_atr, abs_adr = _abs_adr, rel_adr = _rel_adr, 
avg_volume = _avg_volume, rel_volume = _rel_volume, dense_volume = _dense_volume, 
abs_change = _abs_change, rel_change = _rel_change, rel_gap = _rel_gap,
convergence = _convergence
WHERE share_id = NEW.share_id AND date = NEW.date; 

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_d_timeframe_trigger
AFTER INSERT ON d_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_d_timeframe();