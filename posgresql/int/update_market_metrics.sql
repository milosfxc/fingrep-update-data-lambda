CREATE OR REPLACE FUNCTION update_market_metrics()
RETURNS TRIGGER AS $$
DECLARE
	_prev_short_interest market_metrics.short_interest%type;
	_short_interest market_metrics.short_interest%type;
	_prev_short_interest_ratio market_metrics.short_interest_ratio%type;
	_short_interest_ratio market_metrics.short_interest_ratio%type;
    _prev_avg_f_volume market_metrics.avg_f_volume%type;
    _avg_f_volume market_metrics.avg_f_volume%type;
    _price d_timeframe.close%type;
    _market_cap market_metrics.market_cap%type;
    _shares_outstanding market_metrics.shares_outstanding%type;
    _common_shares_outstanding market_metrics.shares_outstanding%type;
    _avg_shares_basic market_metrics.shares_outstanding%type;
BEGIN
-- PREVIOUS SHORT INTEREST VALUES IF CURRENT VALUES ARE NULL
SELECT short_interest, short_interest_ratio, avg_f_volume INTO _prev_short_interest, _prev_short_interest_ratio, _prev_avg_f_volume FROM market_metrics
WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;
-- SHORT INTEREST
IF NEW.short_interest IS NULL THEN
    _short_interest := _prev_short_interest;
ELSE
    _short_interest := NEW.short_interest;
END IF;
-- SHORT INTEREST RATIO
IF NEW.short_interest_ratio IS NULL THEN
   _short_interest_ratio := _prev_short_interest_ratio;
ELSE
   _short_interest_ratio := NEW.short_interest_ratio;
END IF;
-- AVG_F_VOLUME
IF NEW.avg_f_volume IS NULL THEN
   _avg_f_volume := _prev_avg_f_volume;
ELSE
   _avg_f_volume := NEW.avg_f_volume;
END IF;
-- PRICE
SELECT close INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date = NEW.date;
-- SHARES OUTSTANDING
SELECT shares_outstanding INTO _shares_outstanding FROM shares_info
WHERE share_id = NEW.share_id AND shares_outstanding IS NOT NULL;
-- COMMON SHARES OUTSTANDING
SELECT common_shares_outstanding INTO _common_shares_outstanding FROM balance_sheet
WHERE share_id = NEW.share_id AND date <= NEW.date AND date > NEW.date - INTERVAL '100 days' AND common_shares_outstanding IS NOT NULL
ORDER BY date DESC LIMIT 1;
-- AVERAGE SHARES OUTSTANDING
SELECT avg_shares_basic INTO _avg_shares_basic FROM income_statement
WHERE share_id = NEW.share_id AND date <= NEW.date AND date > NEW.date - INTERVAL '100 days' AND avg_shares_basic IS NOT NULL
ORDER BY date DESC LIMIT 1;
-- SHARES OUTSTANDING
IF _common_shares_outstanding > 0 THEN
   _shares_outstanding := _common_shares_outstanding;
ELSIF _avg_shares_basic > 0 THEN
   _shares_outstanding := _avg_shares_basic;
END IF;
-- MARKET CAP
IF _shares_outstanding > 0 AND _price > 0 THEN
   _market_cap := _shares_outstanding * _price / 10000;
END IF;

INSERT INTO market_metrics (
    share_id,
    date,
    avg_f_volume,
    market_cap,
    shares_outstanding,
    short_interest,
    short_interest_ratio
) VALUES (
    NEW.share_id,
    NEW.date,
    _avg_f_volume,
    _market_cap,
    _shares_outstanding,
    _short_interest,
    _short_interest_ratio
         ) ON CONFLICT (share_id, date) DO UPDATE SET
    share_id = EXCLUDED.share_id,
    date = EXCLUDED.date,
    avg_f_volume = EXCLUDED.avg_f_volume,
    market_cap = EXCLUDED.market_cap,
    shares_outstanding = EXCLUDED.shares_outstanding,
    short_interest = EXCLUDED.short_interest,
    short_interest_ratio = EXCLUDED.short_interest_ratio;
RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_market_metrics_trigger
AFTER INSERT ON market_metrics
FOR EACH ROW
EXECUTE FUNCTION update_market_metrics();