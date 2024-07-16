CREATE OR REPLACE FUNCTION update_ratios_after_balance_sheet()
RETURNS TRIGGER AS $$
DECLARE
    _price d_timeframe.close%type;
    _bvps ratios.bvps%type;
    _pb ratios.pb%type;
    _quick_ratio ratios.quick_ratio%type;
    _current_ratio ratios.current_ratio%type;
    _debt_equity ratios.debt_equity%type;
    _lt_debt_equity ratios.lt_debt_equity%type;
    --temp
    _shs_out NUMERIC;
BEGIN
--Price
SELECT close INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date <= NEW.date ORDER BY date DESC LIMIT 1;

--SHARES OUTSTANDING
SELECT common_shares_outstanding INTO _shs_out FROM trade_info WHERE share_id = NEW.share_id AND date = NEW.date;

--BOOK VALUE PER COMMON SHARE
IF NEW.equity is NOT NULL and NEW.preferred_stock_equity IS NOT NULL AND _shs_out IS NOT NULL AND _shs_out <> 0 THEN
    _bvps := ROUND((NEW.equity - NEW.preferred_stock_equity) / _shs_out, 2);
ELSE
    _bvps := NULL;
END IF;

--PRICE TO BOOK
IF _price IS NOT NULL AND _bvps IS NOT NULL AND _bvps <> 0 THEN
	_pb := ROUND(_price / _bvps, 2);
ELSE
	_pb := NULL;
END IF;

--CASH PER COMMON SHARE
IF NEW.cash_and_short_term_investments IS NOT NULL AND _shs_out IS NOT NULL AND _shs_out <> 0 THEN
    _cps := ROUND(NEW.cash_and_short_term_investments / _shs_out, 2);
ELSE
    _cps := NULL;
END IF;

--QUICK RATIO
IF NEW.cash_and_short_term_investments IS NOT NULL AND NEW.net_receivables IS NOT NULL AND NEW.current_liabilities IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _quick_ratio := ROUND((NEW.cash_and_short_term_investments + NEW.net_receivables) / NEW.current_liabilities, 2);
ELSE
    _quick_ratio := NULL;
END IF;

--CURRENT RATIO
IF NEW.current_assets IS NOT NULL AND NEW.current_liabilities IS NOT NULL AND NEW.current_liabilities <> 0 THEN
    _current_ratio := ROUND(NEW.current_assets / NEW.current_liabilities, 2);
ELSE
    _current_ratio := NULL;
END IF;

--DEBT TO EQUITY
IF NEW.liabilities IS NOT NULL AND NEW.equity IS NOT NULL <> NEW.equity <> 0 THEN
    _debt_equity := ROUND(NEW.liabilities / NEW.equity, 2);
ELSE
    _debt_equity := NULL;
END IF;

--LONG-TERM DEBT TO EQUITY
IF NEW.non_current_liabilities IS NOT NULL AND NEW.equity IS NOT NULL <> NEW.equity <> 0 THEN
    _lt_debt_equity := ROUND(NEW.non_current_liabilities / NEW.equity, 2);
ELSE
    _lt_debt_equity := NULL;
END IF;


-- Upsert
INSERT INTO ratios (share_id, date, bvps, pb, quick_ratio, current_ratio, debt_equity, lt_debt_equity)
VALUES (NEW.share_id, NEW.date, _bvps, _pb, _quick_ratio, _current_ratio, _debt_equity, _lt_debt_equity)
ON CONFLICT (share_id, date) DO UPDATE SET
    bvps = EXCLUDED.bvps,
    pb = EXCLUDED.pb,
    quick_ratio = EXCLUDED.quick_ratio,
    current_ratio = EXCLUDED.current_ratio,
    debt_equity = EXCLUDED.debt_equity,
    lt_debt_equity = EXCLUDED.lt_debt_equity;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_ratios_after_balance_sheet
AFTER INSERT ON balance_sheet
FOR EACH ROW
EXECUTE FUNCTION update_ratios_after_balance_sheet();