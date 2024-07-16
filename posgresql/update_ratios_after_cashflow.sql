CREATE OR REPLACE FUNCTION update_ratios_after_cashflow()
RETURNS TRIGGER AS $$
DECLARE
    _price d_timeframe.close%type;
    _pcf ratios.pcf%type;
    _pfcf ratios.free_cash_flow%type;
    _dividend_yield ratios.dividend_yield%type;
    _dividend_payout_ratio ratios.dividend_payout_ratio%type;
    --calculated
    _wei_shs_out NUMERIC;
    _shs_out NUMERIC;
    _ocfps NUMERIC;
    _market_cap NUMERIC;
    _net_income NUMERIC;
BEGIN
--Price
SELECT close INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date <= NEW.date ORDER BY date DESC LIMIT 1;

--WEIGHTED SHARES OUTSTANDING
SELECT weighted_avg_shares_outstanding INTO _wei_shs_out FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date;

--SHARES OUTSTANDING
SELECT common_shares_outstanding INTO _shs_out FROM trade_info WHERE share_id = NEW.share_id AND date = NEW.date;

--OPERATING CASH FLOW PER SHARE
IF NEW.operating_cash_flow IS NOT NULL AND _wei_shs_out IS NOT NULL AND _wei_shs_out <> 0 THEN
	_ocfps := ROUND(NEW.operating_cash_flow / _wei_shs_out, 2);
ELSE
	_ocfps := NULL;
END IF;

--PCF
IF _price IS NOT NULL AND _ocfps IS NOT NULL AND _ocfps <> 0 THEN
	_pcf := ROUND(_price / _ocfps, 2);
ELSE
	_pcf := NULL;
END IF;

--MARKET CAP
IF _shs_out IS NOT NULL AND _shs_out > 0 AND _price IS NOT NULL AND _price > 0 THEN
    _market_cap := _shs_out * _price;
ELSE
    _market_cap := NULL;
END IF;
--PRICE TO FREE CASH FLOW
IF _market_cap IS NOT NULL AND NEW.free_cash_flow IS NOT NULL THEN
    _pfcf := ROUND(_market_cap / NEW.free_cash_flow, 2);
ELSE
    _pfcf := NULL;
END IF;

--DIVIDEND YIELD
IF NEW.dividends_paid IS NOT NULL AND NEW.dividends_paid > 0 AND  _market_cap IS NOT NULL THEN
    _dividend_yield := ROUND((NEW.dividends_paid / _market_cap) * 100, 2);
ELSE
    _dividend_yield := 0;
END IF;

--DIVIDEND PAYOUT RATIO
IF NEW.dividends_paid IS NOT NULL AND NEW.dividends_paid > 0 AND _net_income IS NOT NULL AND _net_income <> 0 THEN
    _dividend_payout_ratio := ROUND(NEW.dividends_paid / _net_income, 2);
ELSE
    _dividend_payout_ratio := NULL;
END IF;


-- Upsert
INSERT INTO ratios (share_id, date, pcf, pfcf, dividend_yield, dividend_payout_ratio)
VALUES (NEW.share_id, NEW.date, _pcf, _pfcf, _dividend_yield, _dividend_payout_ratio)
ON CONFLICT (share_id, date) DO UPDATE SET
    pe = EXCLUDED.pe,
    pcf = EXCLUDED.pcf,
    pcfc = EXCLUDED.pfcf,
    dividend_yield = EXCLUDED.dividend_yield,
    dividend_payout_ratio = EXCLUDED.dividend_payout_ratio;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_cashflow
AFTER INSERT ON cashflow
FOR EACH ROW
EXECUTE FUNCTION update_ratios_after_cashflow();