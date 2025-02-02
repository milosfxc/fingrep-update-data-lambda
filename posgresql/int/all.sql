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


CREATE OR REPLACE FUNCTION update_ratios()
RETURNS TRIGGER AS $$
DECLARE
    --NEW
	_magn BIGINT := 10000;
    _pe ratios.pe%type;
    _ps ratios.ps%type;
    _bvps ratios.bvps%type;
    _pb ratios.pb%type;
    _pcf ratios.pcf%type;
    _pfcf ratios.pfcf%type;
    _m_cap ratios.m_cap%type;
    _eps_yoy ratios.eps_yoy%type;
    _revenue_yoy ratios.revenue_yoy%type;
    _ebitda_yoy ratios.ebitda_yoy%type;
    _net_income_yoy ratios.net_income_yoy%type;
    _cps ratios.cps%type;
    _quick_ratio ratios.quick_ratio%type;
    _current_ratio ratios.current_ratio%type;
    _debt_equity ratios.debt_equity%type;
    _lt_debt_equity ratios.lt_debt_equity%type;
    _roa ratios.roa%type;
    _roe ratios.roe%type;
    _gross_margin ratios.gross_margin%type;
    _operating_margin ratios.operating_margin%type;
    _ebitda_margin ratios.ebitda_margin%type;
    _net_profit_margin ratios.net_profit_margin%type;
    _dividend_yield ratios.dividend_yield%type;
    _dividend_payout_ratio ratios.dividend_payout_ratio%type;
    --EXISTING
    _price d_timeframe.close%type;
    _eps income_statement.eps%type;
    _wei_sh_out income_statement.weighted_avg_shares_outstanding%type;
    _sh_out trade_info.common_shares_outstanding%type;
    _revenue income_statement.revenue%type;
    _ocf cash_flow.operating_cash_flow%type;
    _fcf cash_flow.free_cash_flow%type;
	_prev_revenue income_statement.revenue%type;
    _ebitda income_statement.ebitda%type;
	_prev_ebitda income_statement.ebitda%type;
    _net_income income_statement.net_income%type;
    _prev_net_income income_statement.net_income%type;
    _gross_profit income_statement.gross_profit%type;
    _ebit income_statement.ebit%type;
    _dividends_paid cash_flow.dividends_paid%type;
    --HELPER
    _sales_per_sh BIGINT;
    _ocfps BIGINT;
    _previous_eps income_statement.eps%type;
BEGIN

/*
    VALUATION RATIOS
*/
--PRICE
SELECT CASE WHEN close > 0 THEN close ELSE NULL END  INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date <= NEW.date ORDER BY date DESC LIMIT 1;

--WEIGHTED SHARE OUTSTANDING
SELECT weighted_avg_shares_outstanding INTO _wei_sh_out FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--SHARES OUTSTANDING
SELECT common_shares_outstanding INTO _sh_out  FROM trade_info WHERE share_id = NEW.share_id AND date = NEW.date;

--EPS
SELECT eps INTO _eps FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--PE
IF _price IS NOT NULL AND _eps <> 0 THEN
	_pe := _price * _magn / _eps;
END IF;

--REVENUE
SELECT revenue INTO _revenue FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--SALES PER SHARE
IF _revenue IS NOT NULL AND _sh_out > 0 THEN
    _sales_per_sh := _revenue * _magn / _sh_out;
ELSIF _revenue IS NOT NULL AND _wei_sh_out > 0 THEN
    _sales_per_sh := _revenue * _magn / _wei_sh_out;
END IF;

--PRICE TO SALES PER SHARE
IF _price IS NOT NULL AND _sales_per_sh <> 0 THEN
    _ps := _price * _magn /_sales_per_sh;
END IF;

--BOOK VALUE PER COMMON SHARE
IF NEW.equity IS NOT NULL AND NEW.preferred_stock_equity IS NOT NULL AND _sh_out > 0 THEN
    _bvps := (NEW.equity - NEW.preferred_stock_equity) * _magn / _sh_out;
ELSIF NEW.equity IS NOT NULL AND NEW.preferred_stock_equity IS NOT NULL AND _wei_sh_out > 0 THEN
    _bvps := (NEW.equity - NEW.preferred_stock_equity) * _magn / _wei_sh_out;
END IF;

--PRICE TO BOOK
IF _price IS NOT NULL AND _bvps <> 0 THEN
	_pb := _price * _magn / _bvps;
END IF;

--OPERATING CASH FLOW
SELECT operating_cash_flow INTO _ocf FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;

--OPERATING CASH FLOW PER SHARE
IF _ocf IS NOT NULL AND _wei_sh_out > 0 THEN
	_ocfps := _ocf * _magn / _wei_sh_out;
ELSIF _ocf IS NOT NULL AND _sh_out > 0 THEN
	_ocfps := _ocf * _magn / _sh_out;
END IF;

--PCF
IF _price IS NOT NULL AND _ocfps <> 0 THEN
	_pcf := _price * _magn / _ocfps;
END IF;

--MARKET CAP
IF _price IS NOT NULL AND _sh_out > 0 AND THEN
    _m_cap := _price * _sh_out;
ELSIF _price IS NOT NULL AND _wei_sh_out > 0 THEN
    _m_cap := _price * _wei_sh_out;
END IF;

--FREE CASH FLOW
SELECT free_cash_flow INTO _fcf FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;

--PRICE TO FREE CASH FLOW
IF _m_cap IS NOT NULL AND _fcf <> 0 THEN
    _pfcf := _m_cap * _magn / _fcf;
END IF;


/*
    YOY RATIOS
*/


--PREVIOUS YEAR EPS
SELECT eps INTO _previous_eps FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date;

--EPS YOY
IF _eps IS NOT NULL AND _previous_eps > 0 THEN
	_eps_yoy := ((_eps * _magn / _previous_eps) - 1) * 100;
END IF;

--PREVIOUS YEAR REVENUE
SELECT revenue INTO _prev_revenue FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--REVENUE YOY
IF _revenue IS NOT NULL AND _prev_revenue > 0 THEN
	_revenue_yoy := ((_revenue * _magn / _prev_revenue) - 1) * 100;
END IF;

--EBITDA AND PREVIOUS YEAR EBITDA
SELECT ebitda INTO _ebitda FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;
SELECT ebitda INTO _prev_ebitda FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--EBITDA YOY
IF _ebitda IS NOT NULL AND _prev_ebitda > 0 THEN
	_ebitda_yoy := ((_ebitda * _magn / _prev_ebitda) - 1) * 100;
END IF;

--NET INCOME AND PREVIOUS YEAR NET INCOME
SELECT net_income INTO _net_income FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;
SELECT net_income INTO _prev_net_income FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--NET INCOME YOY
IF _net_income IS NOT NULL AND _prev_net_income > 0 THEN
	_net_income_yoy := ((_net_income * _magn / _prev_net_income) - 1) * 100;
END IF;


/*
    FINANCIAL STRENGTH RATIOS
*/


--CASH PER COMMON SHARE
IF NEW.cash_and_short_term_investments IS NOT NULL AND _sh_out > 0 THEN
    _cps := NEW.cash_and_short_term_investments * _magn / _sh_out;
ELSIF NEW.cash_and_short_term_investments IS NOT NULL AND _wei_sh_out > 0 THEN
    _cps := NEW.cash_and_short_term_investments * _magn / _wei_sh_out;
END IF;

--QUICK RATIO
IF NEW.cash_and_short_term_investments IS NOT NULL AND NEW.net_receivables IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _quick_ratio := (NEW.cash_and_short_term_investments + NEW.net_receivables) * _magn / NEW.current_liabilities;
END IF;

--CURRENT RATIO
IF NEW.current_assets IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _current_ratio := NEW.current_assets * _magn / NEW.current_liabilities;
END IF;

--DEBT TO EQUITY
IF NEW.liabilities IS NOT NULL AND NEW.equity <> 0 THEN
    _debt_equity := NEW.liabilities * _magn / NEW.equity;
END IF;

--LONG-TERM DEBT TO EQUITY
IF NEW.non_current_liabilities IS NOT NULL AND NEW.equity <> 0 THEN
    _lt_debt_equity := NEW.non_current_liabilities * _magn / NEW.equity;
END IF;


/*
    PROFITABILITY RATIOS
*/


--ROA
IF _net_income IS NOT NULL AND NEW.assets > 0 THEN
    _roa := (_net_income * _magn / NEW.assets) * 100;
END IF;

--ROE
IF _net_income IS NOT NULL AND NEW.equity <> 0 THEN
    _roe := (_net_income * _magn / NEW.equity) * 100;
END IF;

--GROSS PROFIT
SELECT gross_profit INTO _gross_profit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--GROSS MARGIN
IF _gross_profit IS NOT NULL AND _revenue > 0 THEN
    _gross_margin := _gross_profit * _magn / _revenue;
END IF;

--EBIT
SELECT ebit INTO _ebit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--OPERATING MARGIN
IF _ebit IS NOT NULL AND _revenue > 0 THEN
    _operating_margin := _ebit * _magn / _revenue;
END IF;

--EBITDA MARGIN
IF _ebitda IS NOT NULL AND _revenue > 0 THEN
    _ebitda_margin := _ebitda * _magn / _revenue;
END IF;

--NET PROFIT MARGIN
IF _net_income IS NOT NULL AND _revenue > 0 THEN
    _net_profit_margin := _net_income * _magn / _revenue;
END IF;


/*
    DIVIDEND RATIOS
*/


--DIVIDENDS PAID
SELECT dividends_paid INTO _dividends_paid FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;


--DIVIDEND YIELD
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND _m_cap > 0 THEN
    _dividend_yield := (_dividends_paid * _magn / _m_cap) * -100;
END IF;

--DIVIDEND PAYOUT RATIO
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND _net_income <> 0 THEN
    _dividend_payout_ratio := ABS(_dividends_paid) * _magn / _net_income;
END IF;


-- UPSERT STATEMENTS
INSERT INTO ratios (
    share_id,
    date,
    pe,
    ps,
    bvps,
    pb,
    pcf,
    pfcf,
    m_cap,
    eps_yoy,
    revenue_yoy,
    ebitda_yoy,
    net_income_yoy,
    cps,
    quick_ratio,
    current_ratio,
    debt_equity,
    lt_debt_equity,
    roa,
    roe,
    gross_margin,
    operating_margin,
    ebitda_margin,
    net_profit_margin,
    dividend_yield,
    dividend_payout_ratio
)
VALUES (
    NEW.share_id,
    NEW.date,
    _pe,
    _ps,
    _bvps,
    _pb,
    _pcf,
    _pfcf,
    _m_cap,
    _eps_yoy,
    _revenue_yoy,
    _ebitda_yoy,
    _net_income_yoy,
    _cps,
    _quick_ratio,
    _current_ratio,
    _debt_equity,
    _lt_debt_equity,
    _roa,
    _roe,
    _gross_margin,
    _operating_margin,
    _ebitda_margin,
    _net_profit_margin,
    _dividend_yield,
    _dividend_payout_ratio
)
ON CONFLICT (share_id, date) DO UPDATE SET
    pe = EXCLUDED.pe,
    ps = EXCLUDED.ps,
    bvps = EXCLUDED.bvps,
    pb = EXCLUDED.pb,
    pcf = EXCLUDED.pcf,
    pfcf = EXCLUDED.pfcf,
    m_cap = EXCLUDED.m_cap,
    eps_yoy = EXCLUDED.eps_yoy,
    revenue_yoy = EXCLUDED.revenue_yoy,
    ebitda_yoy = EXCLUDED.ebitda_yoy,
    net_income_yoy = EXCLUDED.net_income_yoy,
    cps = EXCLUDED.cps,
    quick_ratio = EXCLUDED.quick_ratio,
    current_ratio = EXCLUDED.current_ratio,
    debt_equity = EXCLUDED.debt_equity,
    lt_debt_equity = EXCLUDED.lt_debt_equity,
    roa = EXCLUDED.roa,
    roe = EXCLUDED.roe,
    gross_margin = EXCLUDED.gross_margin,
    operating_margin = EXCLUDED.operating_margin,
    ebitda_margin = EXCLUDED.ebitda_margin,
    net_profit_margin = EXCLUDED.net_profit_margin,
    dividend_yield = EXCLUDED.dividend_yield,
    dividend_payout_ratio = EXCLUDED.dividend_payout_ratio;

INSERT INTO trade_info (
    share_id,
    date,
    weighted_avg_shares_outstanding
    ) VALUES (
    NEW.share_id,
    NEW.date,
    _wei_sh_out
    ) ON CONFLICT (share_id, date) DO UPDATE SET
    weighted_avg_shares_outstanding = EXCLUDED.weighted_avg_shares_outstanding;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_balance_sheet
AFTER INSERT ON balance_sheet
FOR EACH ROW
EXECUTE FUNCTION update_ratios();