CREATE OR REPLACE FUNCTION update_market_breadth(arg_date DATE)
RETURNS VOID AS $$
DECLARE
    _date market_breadth.date%type := arg_date;
    _four_up market_breadth.four_up%type;
    _four_down market_breadth.four_down%type;

    _five_day_ratio market_breadth.five_day_ratio%type;
    _teen_day_ratio market_breadth.teen_day_ratio%type;
    _up25quarter market_breadth.up25quarter%type;
    _down25quarter market_breadth.down25quarter%type;
BEGIN

--4% RATIO
SELECT COUNT(*) INTO _four_up FROM d_timeframe WHERE date = _date AND (avg_volume >= 100000 OR avg_dollar_volume >= 250000) AND rel_change >= 4;
SELECT COUNT(*) INTO _four_down FROM d_timeframe WHERE date = _date AND (avg_volume >= 100000 OR avg_dollar_volume >= 250000) AND rel_change <= -4;

--5 DAY RATIO
WITH last_4 AS (
    SELECT four_up, four_down
    FROM market_breadth
    WHERE date <= _date
    ORDER BY date DESC
    LIMIT 4
)
SELECT
    CASE WHEN COUNT(*) = 4
         THEN (SUM(four_up) + _four_up) / NULLIF(SUM(four_down) + _four_down, 0)
         ELSE NULL
    END INTO _five_day_ratio
FROM last_4;

--10 DAY RATIO
WITH last_9 AS (
    SELECT four_up, four_down
    FROM market_breadth
    WHERE date <= _date
    ORDER BY date DESC
    LIMIT 9
)
SELECT
    CASE WHEN COUNT(*) = 9
         THEN (SUM(four_up) + _four_up) / NULLIF(SUM(four_down) + _four_down, 0)
         ELSE NULL
    END INTO _teen_day_ratio
FROM last_9;

--25% QUARTER
WITH q_data AS (
    SELECT
        share_id,
        date,
		avg_dollar_volume,
		avg_volume,
        close AS end_price,
        MIN(close) OVER (PARTITION BY share_id) AS min_price,
		MAX(close) OVER (PARTITION BY share_id) AS max_price
    FROM
        d_timeframe
    WHERE
        date >= _date::DATE - INTERVAL '3 MONTH'
), q_perf AS (
	SELECT
	    share_id,
	    date,
	    end_price / NULLIF(min_price, 0) AS up_from_qtr_low,
		end_price / NULLIF(max_price, 0) AS down_from_qtr_high
	FROM
	    q_data
	WHERE
	    min_price IS NOT NULL AND min_price > 0 AND date = _date
	)
    SELECT
        COUNT(*) FILTER (WHERE up_from_qtr_low >= 1.25),
        COUNT(*) FILTER (WHERE down_from_qtr_high <= 0.75) INTO _up25quarter, _down25quarter
    FROM
        q_perf;


INSERT INTO market_breadth (
    date,
    four_up,
    four_down,
    five_day_ratio,
    teen_day_ratio,
    up25quarter,
    down25quarter
    ) VALUES (
    _date,
    _four_up,
    _four_down,
    _five_day_ratio,
    _teen_day_ratio,
    _up25quarter,
    _down25quarter
    ) ON CONFLICT (date) DO UPDATE SET
    four_up = EXCLUDED.four_up,
    four_down = EXCLUDED.four_down,
    five_day_ratio = EXCLUDED.five_day_ratio,
    teen_day_ratio = EXCLUDED.teen_day_ratio,
    up25quarter = EXCLUDED.up25quarter,
    down25quarter = EXCLUDED.down25quarter;

    RETURN;
END;
$$ LANGUAGE plpgsql;