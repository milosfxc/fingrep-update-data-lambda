CREATE OR REPLACE FUNCTION update_ratios_after_income_statement()
RETURNS TRIGGER AS $$
DECLARE
    _price d_timeframe.close%type;
    _pe ratios.pe%type;
    _eps_yoy ratios.eps_yoy%type;
    _ps ratios.ps%type;
    _roa ratios.roa%type;
    _roe ratios.roa%type;
    _gross_margin ratios.gross_margin%type;
    _operating_margin ratios.operating_margin%type;
    _ebitda_margin ratios.ebitda_margin%type;
    _net_profit_margin ratios.net_profit_margin%type;
    --calculated
    _previous_eps NUMERIC;
    _assets NUMERIC;
    _equity NUMERIC;
BEGIN
--Price
SELECT close INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date <= NEW.date ORDER BY date DESC LIMIT 1;

--PE
IF NEW.eps IS NOT NULL AND NEW.eps <> 0 AND _price IS NOT NULL THEN
	_pe := ROUND(_price / NEW.eps, 2);
ELSE
	_pe := NULL;
END IF;

--PREVIOUS YEAR EPS
SELECT eps INTO _previous_eps FROM income_statement WHERE share_id = NEW.share_id AND date < NEW.date AND period = 'A' ORDER BY date DESC LIMIT 1;
--EPS YOY
IF _previous_eps IS NOT NULL AND _previous_eps > 0 AND NEW.eps IS NOT NULL THEN
	_eps_yoy := ROUND(((NEW.eps / _previous_eps) - 1) * 100, 2);
ELSE
	_eps_yoy := NULL;
END IF;
--P/S
IF NEW.weighted_avg_shares_outstanding IS NOT NULL AND NEW.weighted_avg_shares_outstanding <> 0 AND NEW.revenue IS NOT NULL THEN
    _ps := ROUND(NEW.revenue / NEW.weighted_avg_shares_outstanding, 2);
ELSE
    _ps := NULL;
END IF;

--ASSETS
SELECT assets INTO _assets FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date;

--ROA
IF NEW.net_income IS NOT NULL AND _assets IS NOT NULL AND _assets <> 0 THEN
    _roa := ROUND((NEW.net_income / _assets) * 100, 2);
ELSE
    _roa := NULL;
END IF;

--EQUITY
SELECT equity INTO _equity FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date;

--ROE
IF NEW.net_income IS NOT NULL AND _equity IS NOT NULL AND _equity <> 0 THEN
    _roe := ROUND((NEW.net_income / _equity) * 100, 2);
ELSE
    _roe := NULL;
END IF;

--GROSS MARGIN
IF NEW.gross_profit IS NOT NULL AND NEW.revenue IS NOT NULL AND NEW.revenue <> 0 THEN
    _gross_margin := ROUND(NEW.gross_profit / NEW.revenue, 2);
ELSE
    _gross_margin := NULL;
END IF;

--OPERATING MARGIN
IF NEW.ebit IS NOT NULL AND NEW.revenue IS NOT NULL AND NEW.revenue <> 0 THEN
    _operating_margin := ROUND(NEW.ebit / NEW.revenue, 2);
ELSE
    _operating_margin := NULL;
END IF;

--EBITDA MARGIN
IF NEW.ebitda IS NOT NULL AND NEW.revenue IS NOT NULL AND NEW.revenue <> 0 THEN
    _ebitda_margin := ROUND(NEW.ebitda / NEW.revenue, 2);
ELSE
    _ebitda_margin := NULL;
END IF;

--NET PROFIT MARGIN
IF NEW.net_income IS NOT NULL AND NEW.revenue IS NOT NULL AND NEW.revenue <> 0 THEN
    _net_profit_margin := ROUND(NEW.net_income / NEW.revenue, 2);
ELSE
    _net_profit_margin := NULL;
END IF;


-- UPSERT
INSERT INTO ratios (share_id, date, pe, eps_yoy, ps, roa, roe, gross_margin, operating_margin, ebitda_margin, net_profit_margin)
VALUES (NEW.share_id, NEW.date, _pe, _eps_yoy, _ps, _roa, _roe, _gross_margin, _operating_margin, _ebitda_margin, _net_profit_margin)
ON CONFLICT (share_id, date) DO UPDATE SET
    pe = EXCLUDED.pe,
    eps_yoy = EXCLUDED.eps_yoy,
    ps = EXCLUDED.ps,
    roa = EXCLUDED.roa,
    roe = EXCLUDED.roe,
    gross_margin = EXCLUDED.gross_margin,
    operating_margin = EXCLUDED.operating_margin,
    ebitda_margin = EXCLUDED.ebitda_margin,
    net_profit_margin = EXCLUDED.net_profit_margin;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_income_statement
AFTER INSERT ON income_statement
FOR EACH ROW
EXECUTE FUNCTION update_ratios_after_income_statement();