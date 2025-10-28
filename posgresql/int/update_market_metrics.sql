CREATE OR REPLACE FUNCTION update_market_metrics()
RETURNS TRIGGER AS $$
DECLARE
	_prev_short_interest market_metrics.short_interest%type;
	_prev_short_interest_ratio market_metrics.short_interest_ratio%type;
    _prev_avg_f_volume market_metrics.avg_f_volume%type;
    _price d_timeframe.close%type;
    _shares_outstanding market_metrics.shares_outstanding%type;
    _common_shares_outstanding market_metrics.shares_outstanding%type;
    _avg_shares_outstanding market_metrics.shares_outstanding%type;
BEGIN
-- PREVIOUS SHORT INTEREST VALUES IF CURRENT VALUES ARE NULL
SELECT short_interest, short_interest_ratio, avg_f_volume INTO _prev_short_interest, _prev_short_interest_ratio, _prev_avg_f_volume FROM market_metrics
WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;
-- SHORT INTEREST
IF NEW.short_interest IS NULL THEN
   NEW.short_interest := _prev_short_interest;
END IF;
-- SHORT INTEREST RATIO
IF NEW.short_interest_ratio IS NULL THEN
   NEW.short_interest_ratio := _prev_short_interest_ratio;
END IF;
-- AVG_F_VOLUME
IF NEW.avg_f_volume IS NULL THEN
   NEW.avg_f_volume := _prev_avg_f_volume;
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
SELECT avg_shares_basic INTO _avg_shares_outstanding FROM balance_sheet
WHERE share_id = NEW.share_id AND date <= NEW.date AND date > NEW.date - INTERVAL '100 days' AND avg_shares_basic IS NOT NULL
ORDER BY date DESC LIMIT 1;
-- SHARES OUTSTANDING
IF _common_shares_outstanding > 0 THEN
   NEW.shares_outstanding := _common_shares_outstanding;
ELSIF _avg_shares_outstanding > 0 THEN
   NEW.shares_outstanding := _avg_shares_outstanding;
ELSIF _shares_outstanding > 0 THEN
   NEW.shares_outstanding := _shares_outstanding;
END IF;
-- MARKET CAP
IF NEW.shares_outstanding > 0 AND _price > 0 THEN
   NEW.market_cap := NEW.shares_outstanding * _price;
END IF;

RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_market_metrics_trigger
BEFORE INSERT ON market_metrics
FOR EACH ROW
EXECUTE FUNCTION update_market_metrics();