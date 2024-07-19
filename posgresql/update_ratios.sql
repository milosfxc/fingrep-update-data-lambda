CREATE OR REPLACE FUNCTION update_ratios()
RETURNS TRIGGER AS $$
DECLARE
    --NEW
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
    _sales_per_sh NUMERIC;
    _ocfps NUMERIC;
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
IF _eps IS NOT NULL AND _eps <> 0 AND _price IS NOT NULL THEN
	_pe := ROUND(_price / _eps, 2);
ELSE
	_pe := NULL;
END IF;

--REVENUE
SELECT revenue INTO _revenue FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--SALES PER SHARE
IF _revenue IS NOT NULL AND _sh_out IS NOT NULL AND _sh_out > 0 THEN
    _sales_per_sh := ROUND(_revenue::NUMERIC / _sh_out::NUMERIC, 2);
ELSIF _revenue IS NOT NULL AND _wei_sh_out IS NOT NULL AND _wei_sh_out > 0 THEN
    _sales_per_sh := ROUND(_revenue::NUMERIC / _wei_sh_out::NUMERIC, 2);
END IF;

--PRICE TO SALES PER SHARE
IF _sales_per_sh IS NOT NULL AND _sales_per_sh <> 0 AND _price IS NOT NULL THEN
    _ps := ROUND(_price/_sales_per_sh::NUMERIC, 2);
END IF;

--BOOK VALUE PER COMMON SHARE
IF NEW.equity is NOT NULL and NEW.preferred_stock_equity IS NOT NULL AND _sh_out IS NOT NULL AND _sh_out > 0 THEN
    _bvps := ROUND((NEW.equity::NUMERIC - NEW.preferred_stock_equity) / _sh_out, 2);
ELSIF NEW.equity is NOT NULL and NEW.preferred_stock_equity IS NOT NULL AND _wei_sh_out IS NOT NULL AND _wei_sh_out > 0 THEN
    _bvps := ROUND((NEW.equity::NUMERIC - NEW.preferred_stock_equity) / _wei_sh_out, 2);
ELSE
    _bvps := NULL;
END IF;

--PRICE TO BOOK
IF _price IS NOT NULL AND _bvps IS NOT NULL AND _bvps <> 0 THEN
	_pb := ROUND(_price::NUMERIC / _bvps, 2);
ELSE
	_pb := NULL;
END IF;

--OPERATING CASH FLOW
SELECT operating_cash_flow INTO _ocf FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;

--OPERATING CASH FLOW PER SHARE
IF _ocf IS NOT NULL AND _wei_sh_out IS NOT NULL AND _wei_sh_out > 0 THEN
	_ocfps := ROUND(_ocf::NUMERIC / _wei_sh_out, 2);
ELSIF _ocf IS NOT NULL AND _sh_out IS NOT NULL AND _sh_out > 0 THEN
	_ocfps := ROUND(_ocf::NUMERIC / _sh_out, 2);
ELSE
	_ocfps := NULL;
END IF;

--PCF
IF _price IS NOT NULL AND _ocfps IS NOT NULL AND _ocfps <> 0 THEN
	_pcf := ROUND(_price::NUMERIC / _ocfps, 2);
ELSE
	_pcf := NULL;
END IF;

--MARKET CAP
IF _sh_out IS NOT NULL AND _sh_out > 0 AND _price IS NOT NULL AND _price > 0 THEN
    _m_cap := _sh_out * _price;
ELSIF _wei_sh_out IS NOT NULL AND _wei_sh_out > 0 AND _price IS NOT NULL AND _price > 0 THEN
    _m_cap := _wei_sh_out * _price;
END IF;

--FREE CASH FLOW
SELECT free_cash_flow INTO _fcf FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;

--PRICE TO FREE CASH FLOW
IF _m_cap IS NOT NULL AND _fcf IS NOT NULL THEN
    _pfcf := ROUND(_m_cap::NUMERIC / _fcf, 2);
ELSE
    _pfcf := NULL;
END IF;


/*
    YEAR OVER YEAR RATIOS
*/


--PREVIOUS YEAR EPS
SELECT eps INTO _previous_eps FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date;

--EPS YOY
IF _previous_eps IS NOT NULL AND _previous_eps > 0 AND _eps IS NOT NULL THEN
	_eps_yoy := ROUND(((_eps::NUMERIC / _previous_eps) - 1) * 100, 2);
ELSE
	_eps_yoy := NULL;
END IF;

--PREVIOUS YEAR REVENUE
SELECT revenue INTO _prev_revenue FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--REVENUE YOY
IF _prev_revenue IS NOT NULL AND _prev_revenue > 0 AND _revenue IS NOT NULL THEN
	_revenue_yoy := ROUND(((_revenue::NUMERIC / _prev_revenue) - 1) * 100, 2);
ELSE
	_revenue_yoy := NULL;
END IF;

--EBITDA AND PREVIOUS YEAR EBITDA
SELECT ebitda INTO _ebitda FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;
SELECT ebitda INTO _prev_ebitda FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--EBITDA YOY
IF _prev_ebitda IS NOT NULL AND _prev_ebitda > 0 AND _ebitda IS NOT NULL THEN
	_ebitda_yoy := ROUND(((_ebitda::NUMERIC / _prev_ebitda::NUMERIC) - 1) * 100, 2);
ELSE
	_ebitda_yoy := NULL;
END IF;

--NET INCOME AND PREVIOUS YEAR NET INCOME
SELECT net_income INTO _net_income FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;
SELECT net_income INTO _prev_net_income FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;

--NET INCOME YOY
IF _prev_net_income IS NOT NULL AND _prev_net_income > 0 AND _net_income IS NOT NULL THEN
	_net_income_yoy := ROUND(((_net_income::NUMERIC / _prev_net_income) - 1) * 100, 2);
ELSE
	_net_income_yoy := NULL;
END IF;


/*
    FINANCIAL STRENGTH RATIOS
*/


--CASH PER COMMON SHARE
IF NEW.cash_and_short_term_investments IS NOT NULL AND _sh_out IS NOT NULL AND _sh_out > 0 THEN
    _cps := ROUND(NEW.cash_and_short_term_investments::NUMERIC / _sh_out, 2);
ELSIF NEW.cash_and_short_term_investments IS NOT NULL AND _wei_sh_out IS NOT NULL AND _wei_sh_out > 0 THEN
    _cps := ROUND(NEW.cash_and_short_term_investments::NUMERIC / _wei_sh_out, 2);
ELSE
    _cps := NULL;
END IF;

--QUICK RATIO
IF NEW.cash_and_short_term_investments IS NOT NULL AND NEW.net_receivables IS NOT NULL AND NEW.current_liabilities IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _quick_ratio := ROUND((NEW.cash_and_short_term_investments::NUMERIC + NEW.net_receivables) / NEW.current_liabilities::NUMERIC, 2);
ELSE
    _quick_ratio := NULL;
END IF;

--CURRENT RATIO
IF NEW.current_assets IS NOT NULL AND NEW.current_liabilities IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _current_ratio := ROUND(NEW.current_assets::NUMERIC / NEW.current_liabilities, 2);
ELSE
    _current_ratio := NULL;
END IF;

--DEBT TO EQUITY
IF NEW.liabilities IS NOT NULL AND NEW.equity IS NOT NULL AND NEW.equity <> 0 THEN
    _debt_equity := ROUND(NEW.liabilities::NUMERIC / NEW.equity, 2);
ELSE
    _debt_equity := NULL;
END IF;

--LONG-TERM DEBT TO EQUITY
IF NEW.non_current_liabilities IS NOT NULL AND NEW.equity IS NOT NULL AND NEW.equity <> 0 THEN
    _lt_debt_equity := ROUND(NEW.non_current_liabilities::NUMERIC / NEW.equity, 2);
ELSE
    _lt_debt_equity := NULL;
END IF;


/*
    PROFITABILITY RATIOS
*/


--ROA
IF _net_income IS NOT NULL AND NEW.assets IS NOT NULL AND NEW.assets > 0 THEN
    _roa := ROUND((_net_income::NUMERIC / NEW.assets) * 100, 2);
ELSE
    _roa := NULL;
END IF;

--ROE
IF _net_income IS NOT NULL AND NEW.equity IS NOT NULL AND NEW.equity <> 0 THEN
    _roe := ROUND((_net_income::NUMERIC / NEW.equity) * 100, 2);
ELSE
    _roe := NULL;
END IF;

--GROSS PROFIT
SELECT gross_profit INTO _gross_profit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--GROSS MARGIN
IF _gross_profit IS NOT NULL AND _revenue IS NOT NULL AND _revenue > 0 THEN
    _gross_margin := ROUND(_gross_profit::NUMERIC / _revenue, 2);
ELSE
    _gross_margin := NULL;
END IF;

--EBIT
SELECT ebit INTO _ebit FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--OPERATING MARGIN
IF _ebit IS NOT NULL AND _revenue IS NOT NULL AND _revenue > 0 THEN
    _operating_margin := ROUND(_ebit::NUMERIC / _revenue, 2);
ELSE
    _operating_margin := NULL;
END IF;

--EBITDA MARGIN
IF _ebitda IS NOT NULL AND _revenue IS NOT NULL AND _revenue > 0 THEN
    _ebitda_margin := ROUND(_ebitda::NUMERIC / _revenue, 2);
ELSE
    _ebitda_margin := NULL;
END IF;

--NET PROFIT MARGIN
IF _net_income IS NOT NULL AND _revenue IS NOT NULL AND _revenue > 0 THEN
    _net_profit_margin := ROUND(_net_income::NUMERIC / _revenue, 2);
ELSE
    _net_profit_margin := NULL;
END IF;


/*
    DIVIDEND RATIOS
*/


--DIVIDENDS PAID
SELECT dividends_paid INTO _dividends_paid FROM cash_flow WHERE share_id = NEW.share_id AND date = NEW.date;


--DIVIDEND YIELD
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND  _m_cap IS NOT NULL AND _m_cap > 0 THEN
    _dividend_yield := ROUND((_dividends_paid::NUMERIC / _m_cap) * -100, 2);
ELSE
    _dividend_yield := NULL;
END IF;

--DIVIDEND PAYOUT RATIO
IF _dividends_paid IS NOT NULL AND _dividends_paid < 0 AND _net_income IS NOT NULL AND _net_income <> 0 THEN
    _dividend_payout_ratio := ROUND(ABS(_dividends_paid::NUMERIC) / _net_income, 2);
ELSE
    _dividend_payout_ratio := NULL;
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