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
    _ev ratios.ev%type;
    _ev_ebitda ratios.ev_ebitda%type;
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
    _avg_sh_out income_statement.avg_shares_basic%type;
    _sh_out balance_sheet.common_shares_outstanding%type;
    _revenue income_statement.revenue%type;
    _ocf cash_flow_statement.operating_cash_flow%type;
    _fcf cash_flow_statement.free_cash_flow%type;
	_prev_revenue income_statement.revenue%type;
    _ebitda income_statement.ebitda%type;
	_prev_ebitda income_statement.ebitda%type;
    _net_income income_statement.net_income%type;
    _prev_net_income income_statement.net_income%type;
    _gross_profit income_statement.gross_profit%type;
    _ebit income_statement.ebit%type;
    _dividends_paid cash_flow_statement.dividends_paid%type;
    --HELPER
    _sales_per_sh BIGINT;
    _ocfps BIGINT;
    _fcfps BIGINT;
    _previous_eps income_statement.eps%type;
BEGIN



/*
    VALUATION RATIOS
*/
--PRICE
SELECT CASE WHEN close > 0 THEN close ELSE NULL END  INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date <= NEW.date ORDER BY date DESC LIMIT 1;

--AVERAGE SHARE OUTSTANDING ANNUAL AND QUARTERLY
SELECT avg_shares_basic INTO _avg_sh_out FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;


--SHARES OUTSTANDING ON DATE
IF NEW.common_shares_outstanding > 0 THEN
    _sh_out := NEW.common_shares_outstanding;
ELSIF _avg_sh_out > 0 THEN
    _sh_out := _avg_sh_out;
ELSE
    RETURN NEW;
END IF;


--SHARES OUTSTANDING FOR PERIOD
IF _avg_sh_out IS NULL OR _avg_sh_out <= 0 THEN
    _avg_sh_out := _sh_out;
END IF;

--MARKET CAP
_m_cap := _price::numeric * _sh_out;

--EPS
SELECT eps INTO _eps FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;

--PE
IF _eps <> 0 THEN
	_pe := _price * _magn / _eps;
END IF;

--REVENUE
SELECT revenue INTO _revenue FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;

--SALES PER SHARE
_sales_per_sh := _revenue * _magn / _avg_sh_out;

--PRICE TO SALES PER SHARE
IF _sales_per_sh <> 0 THEN
    _ps := _price * _magn / _sales_per_sh;
END IF;

--BOOK VALUE PER SHARE
_bvps := NEW.shareholders_equity * _magn / _sh_out;

--PRICE TO BOOK
IF _bvps <> 0 THEN
	_pb := _price * _magn / _bvps;
END IF;

--OPERATING CASH FLOW
SELECT operating_cash_flow INTO _ocf FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;

--OPERATING CASH FLOW PER SHARE
_ocfps := _ocf * _magn::numeric / _avg_sh_out;

--PCF
IF _ocfps <> 0 THEN
	_pcf := _price * _magn / _ocfps;
END IF;

--FREE CASH FLOW
SELECT free_cash_flow INTO _fcf FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;

--FREE CASH FLOW PER SHARE
_fcfps := _fcf * _magn::numeric / _avg_sh_out;

--PRICE TO FREE CASH FLOW PER SHARE
IF _fcfps <> 0 THEN
    _pfcf := _price * _magn / _fcfps;
END IF;

--ENTERPRISE VALUE
IF NEW.cash_and_short_term_investments > 0 AND NEW.debt > 0 AND _m_cap > 0 THEN
    _ev := _m_cap - NEW.cash_and_short_term_investments + NEW.debt;
END IF;

--EV/EBITDA
IF _ev > 0 AND NEW.ebitda > 0 THEN
    _ev_ebitda := _ev / NEW.ebitda;
END IF;

/*
    YOY RATIOS
*/

--PREVIOUS PERIOD EPS
SELECT eps INTO _previous_eps FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date AND report_type = NEW.report_type ORDER BY date DESC LIMIT 1;

--EPS YOY
IF _eps > 0 AND _previous_eps > 0 THEN
	_eps_yoy := ((_eps * _magn / _previous_eps) - 10000) * 100;
END IF;

--PREVIOUS YEAR REVENUE
SELECT revenue INTO _prev_revenue FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date AND report_type = NEW.report_type ORDER BY date DESC LIMIT 1;

--REVENUE YOY
IF _revenue > 0 AND _prev_revenue > 0 THEN
	_revenue_yoy := ((_revenue * _magn / _prev_revenue) - 10000) * 100;
END IF;

--EBITDA AND PREVIOUS YEAR EBITDA
SELECT ebitda INTO _ebitda FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;
SELECT ebitda INTO _prev_ebitda FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date AND report_type = NEW.report_type ORDER BY date DESC LIMIT 1;

--EBITDA YOY
IF _ebitda > 0 AND _prev_ebitda > 0 THEN
	_ebitda_yoy := ((_ebitda::numeric * _magn / _prev_ebitda) - 10000) * 100;
END IF;

--NET INCOME AND PREVIOUS YEAR NET INCOME
SELECT net_income INTO _net_income FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;
SELECT net_income INTO _prev_net_income FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date AND report_type = NEW.report_type ORDER BY date DESC LIMIT 1;

--NET INCOME YOY
IF _net_income > 0 AND _prev_net_income > 0 THEN
	_net_income_yoy := ((_net_income::numeric * _magn / _prev_net_income) - 10000) * 100;
END IF;


/*
    FINANCIAL STRENGTH RATIOS
*/


--CASH PER COMMON SHARE
_cps := NEW.cash_and_short_term_investments * _magn / _avg_sh_out;

--QUICK RATIO
IF NEW.cash_and_short_term_investments IS NOT NULL AND NEW.net_receivables IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _quick_ratio := (NEW.cash_and_short_term_investments + NEW.net_receivables)::numeric * _magn / NEW.current_liabilities;
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
    _lt_debt_equity := NEW.non_current_liabilities::numeric * _magn / NEW.equity;
END IF;

/*
    PROFITABILITY RATIOS
*/

--ROA
IF _net_income IS NOT NULL AND NEW.assets > 0 THEN
    _roa := (_net_income::numeric * _magn / NEW.assets) * 100;
END IF;

--ROE
IF _net_income IS NOT NULL AND NEW.equity <> 0 THEN
    _roe := (_net_income::numeric * _magn / NEW.equity) * 100;
END IF;

--GROSS PROFIT
SELECT gross_profit INTO _gross_profit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;

--GROSS MARGIN
IF _gross_profit IS NOT NULL AND _revenue > 0 THEN
    _gross_margin := (_gross_profit::numeric * _magn / _revenue) * 100;
END IF;

--EBIT
SELECT ebit INTO _ebit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND report_type = NEW.report_type;

--OPERATING MARGIN
IF _ebit IS NOT NULL AND _revenue > 0 THEN
    _operating_margin := (_ebit::numeric * _magn / _revenue) * 100;
END IF;

--EBITDA MARGIN
IF _ebitda IS NOT NULL AND _revenue > 0 THEN
    _ebitda_margin := (_ebitda::numeric * _magn / _revenue) * 100;
END IF;

--NET PROFIT MARGIN
IF _net_income IS NOT NULL AND _revenue > 0 THEN
    _net_profit_margin := (_net_income::numeric * _magn / _revenue) * 100;
END IF;


/*
    DIVIDEND RATIOS
*/


--DIVIDENDS PAID
SELECT dividends_paid INTO _dividends_paid FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type;


--DIVIDEND YIELD
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND _m_cap > 0 THEN
    _dividend_yield := (ABS(_dividends_paid) * _magn / _m_cap) * 100;
END IF;

--DIVIDEND PAYOUT RATIO
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND _net_income <> 0 THEN
    _dividend_payout_ratio := (ABS(_dividends_paid) * _magn / _net_income) * 100;
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
    ev,
    ev_ebitda,
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
    dividend_payout_ratio,
    report_type,
    filing_date,
    calendar_period_id,
    fiscal_period_id
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
    _ev,
    _ev_ebitda,
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
    _dividend_payout_ratio,
    NEW.report_type,
    NEW.filing_date,
    NEW.calendar_period_id,
    NEW.fiscal_period_id
)
ON CONFLICT (share_id, date) DO UPDATE SET
    pe = EXCLUDED.pe,
    ps = EXCLUDED.ps,
    bvps = EXCLUDED.bvps,
    pb = EXCLUDED.pb,
    pcf = EXCLUDED.pcf,
    pfcf = EXCLUDED.pfcf,
    ev = EXCLUDED.ev,
    ev_ebitda = EXCLUDED.ev_ebitda,
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
    dividend_payout_ratio = EXCLUDED.dividend_payout_ratio,
    report_type = EXCLUDED.report_type,
    filing_date = EXCLUDED.filing_date,
    calendar_period_id = EXCLUDED.calendar_period_id,
    fiscal_period_id = EXCLUDED.fiscal_period_id;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_balance_sheet
AFTER INSERT ON balance_sheet
FOR EACH ROW
EXECUTE FUNCTION update_ratios();