CREATE OR REPLACE FUNCTION update_market_metrics()
RETURNS TRIGGER AS $$
DECLARE
	_prev_short_interest market_metrics.short_interest%type;
	_prev_short_interest_ratio market_metrics.short_interest_ratio%type;
    _prev_avg_f_volume market_metrics.avg_f_volume%type;
    _price d_timeframe.close%type;
    _shares_outstanding market_metrics.shares_outstanding%type;
    _common_shares_outstanding market_metrics.shares_outstanding%type;
    _avg_shares_basic market_metrics.shares_outstanding%type;
BEGIN
-- PREVIOUS SHORT INTEREST VALUES IF CURRENT VALUES ARE NULL
SELECT short_interest, short_interest_ratio, avg_f_volume INTO _prev_short_interest, _prev_short_interest_ratio, _prev_avg_f_volume FROM market_metrics
WHERE share_id = NEW.share_id AND date < NEW.date ORDER BY date DESC LIMIT 1;
-- SHORT INTEREST
NEW.short_interest := COALESCE(NEW.short_interest, _prev_short_interest);
-- SHORT INTEREST RATIO
NEW.short_interest_ratio := COALESCE(NEW.short_interest_ratio, _prev_short_interest_ratio);
-- AVG_F_VOLUME
NEW.avg_f_volume := coalesce(NEW.avg_f_volume, _prev_avg_f_volume);
-- PRICE
SELECT close INTO _price FROM d_timeframe WHERE share_id = NEW.share_id AND date = NEW.date;
-- SHARES OUTSTANDING
SELECT shares_outstanding INTO _shares_outstanding FROM shares_info
WHERE share_id = NEW.share_id AND shares_outstanding IS NOT NULL AND shares_outstanding > 0;
-- COMMON SHARES OUTSTANDING
SELECT common_shares_outstanding INTO _common_shares_outstanding FROM balance_sheet
WHERE share_id = NEW.share_id AND date <= NEW.date AND date > NEW.date - INTERVAL '135 days' AND common_shares_outstanding IS NOT NULL
ORDER BY date DESC LIMIT 1;
-- AVERAGE SHARES OUTSTANDING
SELECT avg_shares_basic INTO _avg_shares_basic FROM income_statement
WHERE share_id = NEW.share_id AND date <= NEW.date AND date > NEW.date - INTERVAL '135 days' AND avg_shares_basic IS NOT NULL
ORDER BY date DESC LIMIT 1;
-- SHARES OUTSTANDING
IF NEW.date < CURRENT_DATE - INTERVAL '30 days' OR _shares_outstanding IS NULL THEN
    IF _common_shares_outstanding > 0 THEN
       _shares_outstanding := _common_shares_outstanding;
    ELSIF _avg_shares_basic > 0 THEN
       _shares_outstanding := _avg_shares_basic;
    END IF;
END IF;
-- SHARES OUTSTANDING
NEW.shares_outstanding := _shares_outstanding;
-- MARKET CAP
IF _shares_outstanding > 0 AND _price > 0 THEN
   NEW.market_cap := _shares_outstanding * _price / 10000;
END IF;

RETURN NEW;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_market_metrics_trigger
BEFORE INSERT OR UPDATE ON market_metrics
FOR EACH ROW
EXECUTE FUNCTION update_market_metrics();