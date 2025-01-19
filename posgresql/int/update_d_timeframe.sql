CREATE OR REPLACE FUNCTION update_d_timeframe()
RETURNS TRIGGER AS $$
DECLARE
	_magn BIGINT := 10000;
    _sma10 d_timeframe.sma10%type;
    _sma20 d_timeframe.sma20%type;
    _sma50 d_timeframe.sma50%type;
    _sma100 d_timeframe.sma100%type;
    _sma200 d_timeframe.sma200%type;
	_rel_atr d_timeframe.rel_atr%type;
	_abs_atr d_timeframe.abs_atr%type;
	_rel_adr d_timeframe.rel_adr%type;
    _abs_adr d_timeframe.abs_adr%type;
	_dollar_volume d_timeframe.dollar_volume%type;
	_avg_volume d_timeframe.avg_volume%type;
	_avg_dollar_volume d_timeframe.avg_dollar_volume%type;
	_rel_volume d_timeframe.rel_volume%type;
	_abs_change d_timeframe.abs_change%type;
	_rel_change d_timeframe.rel_change%type;
	_rel_gap d_timeframe.rel_gap%type;
	_dense_volume d_timeframe.dense_volume%type;
	_avg_volume_40 BIGINT;
	_avg_volume_ytd BIGINT;
	_convergence d_timeframe.convergence%type;
	_market_cap shares_info.market_cap%type;
	_rel_change_from_open d_timeframe.rel_change_from_open%type;
	_rel_w_change d_timeframe.rel_w_change%type;
	_rel_m_change d_timeframe.rel_m_change%type;
	_rel_q_change d_timeframe.rel_q_change%type;
	_rel_6m_change d_timeframe.rel_6m_change%type;
	_rel_y_change d_timeframe.rel_y_change%type;
	_rel_ytd_change d_timeframe.rel_ytd_change%type;
    _twenty_day_low d_timeframe.twenty_day_low%type;
	_twenty_day_high d_timeframe.twenty_day_high%type;
    _fifty_day_low d_timeframe.fifty_day_low%type;
	_fifty_day_high d_timeframe.fifty_day_high%type;
	_ytd_low d_timeframe.ytd_high%type;
	_ytd_high d_timeframe.ytd_low%type;
	_all_time_low d_timeframe.all_time_low%type;
	_all_time_high d_timeframe.all_time_high%type;
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
SELECT CASE WHEN (SELECT COUNT(*) FROM last_10) = 10 THEN AVG(close) END INTO _sma10 FROM last_10;

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
	CASE WHEN (SELECT COUNT(*) FROM last_14) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END INTO _abs_atr
FROM
    last_14;

SELECT CASE WHEN _abs_atr IS NOT NULL AND NEW.close <> 0 THEN _abs_atr * _magn / NEW.close * 100 END INTO _rel_atr;

--DOLLAR VOLUME
IF NEW.volume IS NOT NULL AND NEW.vwap IS NOT NULL THEN
    _dollar_volume := NEW.volume * NEW.vwap;
ELSE
    _dollar_volume := 0;
END IF;

--SMA20 & ADR & AVGVOL & TWENTY DAY HIGH/LOW
WITH last_20 AS (
	SELECT high, low, close, volume, vwap
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 20
),
row_count AS (
    SELECT COUNT(*) AS cnt FROM last_20
)
SELECT
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN 100 * (AVG(high * _magn / low) - 10000) END AS _rel_adr,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN AVG(high - low) END AS _abs_adr,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN AVG(close) END AS _sma20,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN AVG(volume) END AS _avg_volume,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN AVG(volume * vwap) END AS _avg_dollar_volume,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN MIN(low) END AS _twenty_day_low,
	CASE WHEN (SELECT cnt FROM row_count) = 20 THEN MAX(high) END AS _twenty_day_high
INTO
	_rel_adr, _abs_adr, _sma20, _avg_volume, _avg_dollar_volume, _twenty_day_low, _twenty_day_high
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
	CASE WHEN (SELECT COUNT(*) FROM last_40) = 40 THEN AVG(volume) ELSE 0 END AS _avg_volume_40
INTO _avg_volume_40
FROM last_40;

WITH last_252 AS (
	SELECT volume, low, high FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 252
),
row_count AS (
    SELECT COUNT(*) AS cnt FROM last_252
)
SELECT
	CASE WHEN (SELECT cnt FROM row_count) = 252 THEN AVG(volume) ELSE 0 END,
	CASE WHEN (SELECT cnt FROM row_count) >= 250 THEN MIN(low) END,
	CASE WHEN (SELECT cnt FROM row_count) >= 250 THEN MAX(high) END
INTO _avg_volume_ytd, _ytd_low, _ytd_high
FROM last_252;

SELECT
	CASE WHEN _avg_volume_40 != 0 AND _avg_volume_ytd != 0 THEN _avg_volume_40 * _magn / _avg_volume_ytd ELSE 0 END AS _dense_volume
INTO _dense_volume;
--RVOL
SELECT CASE WHEN _avg_volume != 0 THEN volume * _magn / _avg_volume END INTO _rel_volume
FROM d_timeframe WHERE share_id = NEW.share_id AND date = NEW.date;
--SMA50 & FIFTY DAY HIGH/LOW
WITH last_50 AS (
	SELECT close, low, high
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 50
),
row_count AS (
    SELECT COUNT(*) AS cnt FROM last_50
)
SELECT
    CASE WHEN (SELECT cnt FROM row_count) = 50 THEN AVG(close) END,
    CASE WHEN (SELECT cnt FROM row_count) = 50 THEN MIN(low) END,
	CASE WHEN (SELECT cnt FROM row_count) = 50 THEN MAX(high) END
INTO _sma50, _fifty_day_low, _fifty_day_high
FROM last_50;

--SMA100
WITH last_100 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 100
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_100) = 100 THEN AVG(close) END INTO _sma100 FROM last_100;

--SMA200
WITH last_200 AS (
	SELECT close
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC
	LIMIT 200
)
SELECT CASE WHEN (SELECT COUNT(*) FROM last_200) = 200 THEN AVG(close) END INTO _sma200 FROM last_200;

--CHANGE FROM PREVIOUS DAY
WITH last_2 AS (
	SELECT date, close, open
	FROM d_timeframe
	WHERE share_id = NEW.share_id
	AND date <= NEW.date
	ORDER BY date DESC LIMIT 2
)
SELECT close - LAG(close, 1) OVER (ORDER BY date ASC) AS _abs_change,
((close * _magn / LAG(close, 1) OVER (ORDER BY date ASC)) - 10000) * 100 AS _rel_change,
((open * _magn / LAG(close, 1) OVER (ORDER BY date ASC)) - 10000) * 100 AS _rel_gap
INTO _abs_change, _rel_change, _rel_gap FROM last_2 ORDER BY date DESC LIMIT 1;

--AFTER HOURS CHANGE
IF _rel_change IS NOT NULL AND _rel_gap IS NOT NULL THEN
	_rel_change_from_open := _rel_change - _rel_gap;
END IF;

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

--MARKET CAP
SELECT
	CASE WHEN shares_outstanding IS NOT NULL
	THEN shares_outstanding * NEW.close
	END
	INTO _market_cap
FROM shares_info;

--WEEKLY CHANGE
SELECT (NEW.close * _magn / open - 10000) * 100 INTO _rel_w_change FROM d_timeframe WHERE date > (NEW.date - INTERVAL '1 week')
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--MONTHLY CHANGE
SELECT (NEW.close * _magn / open - 10000) * 100 INTO _rel_m_change FROM d_timeframe WHERE date > (NEW.date - INTERVAL '1 month')
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--QUARTERLY CHANGE
SELECT (NEW.close * _magn / open - 10000) * 100 INTO _rel_q_change FROM d_timeframe WHERE date > (NEW.date - INTERVAL '3 month')
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--6M CHANGE
SELECT (NEW.close * _magn / open - 10000) * 100 INTO _rel_6m_change FROM d_timeframe WHERE date > (NEW.date - INTERVAL '6 month')
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--YTD CHANGE
SELECT (NEW.close * _magn / open - 10000) * 100 INTO _rel_ytd_change FROM d_timeframe WHERE date > (NEW.date - INTERVAL '1 year')
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--YEAR CHANGE
SELECT (NEW.close * _magn / close - 10000) * 100 INTO _rel_y_change FROM d_timeframe WHERE date >= DATE_TRUNC('year', NEW.date)
AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;

--ALL TIME HIGH/LOW
SELECT
	MIN(low),
	MAX(high)
INTO _all_time_low, _all_time_high
FROM d_timeframe
WHERE share_id = NEW.share_id
AND date <= NEW.date;

--UPDATE
UPDATE d_timeframe SET sma10 = _sma10, sma20 = _sma20, sma50 = _sma50, sma100 = _sma100, sma200 = _sma200,
abs_atr = _abs_atr, rel_atr = _rel_atr, abs_adr = _abs_adr, rel_adr = _rel_adr,
dollar_volume = _dollar_volume, avg_volume = _avg_volume, avg_dollar_volume = _avg_dollar_volume, rel_volume = _rel_volume, dense_volume = _dense_volume,
abs_change = _abs_change, rel_change = _rel_change, rel_gap = _rel_gap, rel_change_from_open = _rel_change_from_open,
convergence = _convergence,
rel_w_change = _rel_w_change, rel_m_change = _rel_m_change, rel_q_change = _rel_q_change, rel_6m_change = _rel_6m_change,
rel_ytd_change = _rel_ytd_change, rel_y_change = _rel_y_change,
twenty_day_low = _twenty_day_low, twenty_day_high = _twenty_day_high, fifty_day_low = _fifty_day_low, fifty_day_high = _fifty_day_high, ytd_low = _ytd_low, ytd_high = _ytd_high, all_time_low = _all_time_low, all_time_high = _all_time_high
WHERE share_id = NEW.share_id AND date = NEW.date;

IF _market_cap IS NOT NULL THEN
	UPDATE shares_info SET market_cap = _market_cap WHERE share_id = NEW.share_id;
END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_d_timeframe_trigger
AFTER INSERT ON d_timeframe
FOR EACH ROW
EXECUTE FUNCTION update_d_timeframe();