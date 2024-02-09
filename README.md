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
CREATE OR REPLACE FUNCTION update_sma_test() 
RETURNS TRIGGER AS $$
DECLARE 
    _sma10 d_timeframe.sma10%type;
    _sma20 d_timeframe.sma20%type;
    _sma50 d_timeframe.sma50%type;
    _sma100 d_timeframe.sma100%type;
    _sma200 d_timeframe.sma200%type;
	_rel_adr d_timeframe.rel_adr%type;
    _abs_adr d_timeframe.abs_adr%type;
	_avg_volume d_timeframe.avg_volume%type;
	_rel_volume d_timeframe.rel_volume%type;
	_abs_change d_timeframe.abs_change%type;
	_rel_change d_timeframe.rel_change%type;
	_rel_gap d_timeframe.rel_gap%type;
BEGIN
--SMA10
WITH last_10 AS (
	SELECT close
	FROM d_timeframe
	WHERE date <= NEW.date AND share_id = NEW.share_id
	ORDER BY date DESC
	LIMIT 10
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN ROUND(AVG(close), 4) END INTO _sma10 FROM last_10;

--SMA20 & ADR & AVGVOL 
WITH last_20 AS (
	SELECT high, low, close, volume
	FROM d_timeframe
	WHERE date <= NEW.date AND share_id = NEW.share_id
	ORDER BY date DESC
	LIMIT 20
)
SELECT 
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(100 * (AVG(high/low) - 1), 2) END AS _rel_adr,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND((AVG(high - low)), 2) END AS _abs_adr,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(AVG(close), 4) END AS _sma20,
	CASE WHEN (SELECT COUNT(*) FROM last_20) = 20 THEN ROUND(AVG(volume)) END AS _avg_volume
INTO 
	_rel_adr, _abs_adr, _sma20, _avg_volume
FROM last_20;

--RVOL
SELECT CASE WHEN _avg_volume IS NOT NULL THEN ROUND(volume::numeric/_avg_volume, 2) END INTO _rel_volume 
FROM d_timeframe WHERE share_id = NEW.share_id AND date = NEW.date;
--SMA50
WITH last_50 AS (
	SELECT close
	FROM d_timeframe
	WHERE date <= NEW.date AND share_id = NEW.share_id
	ORDER BY date DESC
	LIMIT 50
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_50) = 50 THEN ROUND(AVG(close), 4) END INTO _sma50 FROM last_50;

--SMA100
WITH last_100 AS (
	SELECT close
	FROM d_timeframe
	WHERE date <= NEW.date AND share_id = NEW.share_id
	ORDER BY date DESC
	LIMIT 100
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_100) = 100 THEN ROUND(AVG(close), 4) END INTO _sma100 FROM last_100;

--SMA200
WITH last_200 AS (
	SELECT close
	FROM d_timeframe
	WHERE date <= NEW.date AND share_id = NEW.share_id
	ORDER BY date DESC
	LIMIT 200
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_200) = 200 THEN ROUND(AVG(close), 4) END INTO _sma200 FROM last_200;

--CHANGE FROM PREVIOUS DAY
WITH last_2 AS (
	SELECT date, close, open 
	FROM d_timeframe
	WHERE share_id = NEW.share_id AND date <= NEW.date
	ORDER BY date DESC LIMIT 2
)
SELECT close - LAG(close, 1) OVER (ORDER BY date ASC) AS _abs_change, 
ROUND(((close/ LAG(close, 1) OVER (ORDER BY date ASC)) - 1) * 100, 2) AS _rel_change,
ROUND(((open/ LAG(close, 1) OVER (ORDER BY date ASC)) - 1) * 100, 2) AS _rel_gap 
INTO _abs_change, _rel_change, _rel_gap FROM last_2 ORDER BY date DESC LIMIT 1;
 
--UPDATE
UPDATE d_timeframe SET sma10 = _sma10, sma20 = _sma20, sma50 = _sma50, sma100 = _sma100, sma200 = _sma200, 
rel_adr = _rel_adr, abs_adr = _abs_adr, 
avg_volume = _avg_volume, rel_volume = _rel_volume, 
abs_change = _abs_change, rel_change = _rel_change, rel_gap = _rel_gap 
WHERE share_id = NEW.share_id AND date = NEW.date; 

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sma_test_trigger
AFTER INSERT ON d_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_sma_test();