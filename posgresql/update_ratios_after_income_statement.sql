CREATE OR REPLACE FUNCTION update_ratios_after_income_statement()
RETURNS TRIGGER AS $$
DECLARE
    _price d_timeframe.close%type;
    _pe ratios.pe%type;
    _eps_yoy ratios.eps_yoy%type;
    _ps ratios.ps%type;
    --calculated
    _previous_eps NUMERIC;
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
-- Upsert
INSERT INTO ratios (share_id, date, pe, eps_yoy, ps)
VALUES (NEW.share_id, NEW.date, _pe, _eps_yoy, _ps)
ON CONFLICT (share_id, date) DO UPDATE SET
    pe = EXCLUDED.pe,
    eps_yoy = EXCLUDED.eps_yoy,
    ps = EXCLUDED.ps;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_income_statement
AFTER INSERT ON income_statement
FOR EACH ROW
EXECUTE FUNCTION update_ratios_after_income_statement();