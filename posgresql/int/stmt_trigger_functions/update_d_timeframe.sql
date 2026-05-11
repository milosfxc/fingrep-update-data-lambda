-- =========================================================================
-- d_timeframe statement-level trigger
--
-- Preserves every indicator from the original row-level trigger:
--   sma10, sma20, sma50, sma100, sma200,
--   abs_atr, rel_atr, abs_adr, rel_adr,
--   dollar_volume, avg_volume, avg_dollar_volume, rel_volume, dense_volume,
--   abs_change, rel_change, rel_gap, rel_change_from_open,
--   convergence,
--   rel_w_change, rel_m_change, rel_q_change, rel_6m_change,
--   rel_ytd_change, rel_y_change,
--   twenty_day_low/high, fifty_day_low/high, ytd_low/high, all_time_low/high
--
-- Plus the rollup into w_timeframe.
--
-- Strategy: every per-row computation from the original is rewritten as a
-- CTE that joins LATERAL from `affected` (= DISTINCT share_id, date pairs
-- in new_rows). Then a single UPDATE writes all indicators in one pass.
-- The w_timeframe rollup reuses the new_rows transition table directly.
--
-- pg_trigger_depth() = 0 guard prevents the indicator self-UPDATE from
-- re-firing the trigger.
-- =========================================================================

CREATE OR REPLACE FUNCTION update_d_timeframe()
RETURNS TRIGGER AS $$
DECLARE
    _magn CONSTANT BIGINT := 10000;
BEGIN

-- ------------------------------------------------------------------------
-- Build all indicators for every affected row in a single statement
-- ------------------------------------------------------------------------
WITH affected AS (
    SELECT DISTINCT share_id, date FROM new_rows
),

-- SMA10
sma10_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- ABS_ATR (14-row window with LAG)
atr_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 13
                THEN AVG(GREATEST(high - low,
                                  ABS(high - prev_close),
                                  ABS(low  - prev_close)))
                END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, LAG(close) OVER (ORDER BY date) AS prev_close
        FROM (
            SELECT date, high, low, close FROM d_timeframe
            WHERE share_id = a.share_id AND date <= a.date
            ORDER BY date DESC LIMIT 14
        ) last14
        ORDER BY date ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- 20-row stats: rel_adr, abs_adr, sma20, avg_volume, avg_dollar_volume,
-- twenty_day_low, twenty_day_high
last20_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) = 20 THEN 100 * (AVG(t.high * _magn / NULLIF(t.low,0)) - 10000) END AS rel_adr,
           CASE WHEN COUNT(*) = 20 THEN AVG(t.high - t.low) END AS abs_adr,
           CASE WHEN COUNT(*) = 20 THEN AVG(t.close) END AS sma20,
           CASE WHEN COUNT(*) = 20 THEN AVG(t.volume) END AS avg_volume,
           CASE WHEN COUNT(*) = 20 THEN AVG(t.volume * t.vwap / _magn) END AS avg_dollar_volume,
           CASE WHEN COUNT(*) = 20 THEN MIN(t.low) END AS twenty_day_low,
           CASE WHEN COUNT(*) = 20 THEN MAX(t.high) END AS twenty_day_high
    FROM affected a
    JOIN LATERAL (
        SELECT high, low, close, volume, vwap FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 20
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- 40-row avg_volume (for dense_volume numerator)
last40_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) = 40 THEN AVG(t.volume) ELSE 0 END AS avg_volume_40
    FROM affected a
    JOIN LATERAL (
        SELECT volume FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 40
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- 252-row stats: avg_volume_ytd (for dense_volume denominator), ytd_low, ytd_high
last252_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) = 252 THEN AVG(t.volume) ELSE 0 END AS avg_volume_ytd,
           CASE WHEN COUNT(*) >= 250 THEN MIN(t.low) END AS ytd_low,
           CASE WHEN COUNT(*) >= 250 THEN MAX(t.high) END AS ytd_high
    FROM affected a
    JOIN LATERAL (
        SELECT volume, low, high FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 252
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- 50-row stats: sma50, fifty_day_low, fifty_day_high
last50_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) = 50 THEN AVG(t.close) END AS sma50,
           CASE WHEN COUNT(*) = 50 THEN MIN(t.low) END AS fifty_day_low,
           CASE WHEN COUNT(*) = 50 THEN MAX(t.high) END AS fifty_day_high
    FROM affected a
    JOIN LATERAL (
        SELECT close, low, high FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 50
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- SMA100
sma100_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(t.close) = 100 THEN AVG(t.close) END AS sma100
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 100
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- SMA200
sma200_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(t.close) = 200 THEN AVG(t.close) END AS sma200
    FROM affected a
    JOIN LATERAL (
        SELECT close FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 200
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- Per-row data: needed for dollar_volume, rel_volume, change vs prev day
current_row AS (
    SELECT d.share_id, d.date, d.open, d.close, d.volume, d.vwap
    FROM affected a
    JOIN d_timeframe d ON d.share_id = a.share_id AND d.date = a.date
),

-- Previous day's close + current day's open for abs_change/rel_change/rel_gap
prev_day_calc AS (
    SELECT a.share_id, a.date,
           t.close      AS curr_close,
           t.open       AS curr_open,
           prev.close   AS prev_close
    FROM affected a
    JOIN d_timeframe t ON t.share_id = a.share_id AND t.date = a.date
    LEFT JOIN LATERAL (
        SELECT close FROM d_timeframe
        WHERE share_id = a.share_id AND date < a.date
        ORDER BY date DESC LIMIT 1
    ) prev ON TRUE
),

-- 3-row convergence
conv_calc AS (
    SELECT a.share_id, a.date,
           CASE WHEN COUNT(*) = 3
                 AND MAX(t.high) = (array_agg(t.high ORDER BY t.date ASC))[1]
                 AND MIN(t.low)  = (array_agg(t.low  ORDER BY t.date ASC))[1]
                THEN true ELSE false END AS convergence
    FROM affected a
    JOIN LATERAL (
        SELECT date, high, low FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
        ORDER BY date DESC LIMIT 3
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- Window changes (week/month/quarter/6m/ytd): all use the earliest "open"
-- after the cutoff vs current close.
-- Original logic: SELECT (NEW.close * _magn / NULLIF(open,0) - 10000) * 100
--   FROM d_timeframe WHERE date > (NEW.date - INTERVAL 'X')
--   AND share_id = NEW.share_id ORDER BY date ASC LIMIT 1;
window_changes AS (
    SELECT
        cr.share_id, cr.date,
        (cr.close * _magn / NULLIF(w.open, 0) - 10000) * 100 AS rel_w_change,
        (cr.close * _magn / NULLIF(m.open, 0) - 10000) * 100 AS rel_m_change,
        (cr.close * _magn / NULLIF(q.open, 0) - 10000) * 100 AS rel_q_change,
        (cr.close * _magn / NULLIF(s.open, 0) - 10000) * 100 AS rel_6m_change,
        (cr.close * _magn / NULLIF(y.open, 0) - 10000) * 100 AS rel_ytd_change
    FROM current_row cr
    LEFT JOIN LATERAL (
        SELECT open FROM d_timeframe
        WHERE share_id = cr.share_id AND date > (cr.date - INTERVAL '1 week')
        ORDER BY date ASC LIMIT 1
    ) w ON TRUE
    LEFT JOIN LATERAL (
        SELECT open FROM d_timeframe
        WHERE share_id = cr.share_id AND date > (cr.date - INTERVAL '1 month')
        ORDER BY date ASC LIMIT 1
    ) m ON TRUE
    LEFT JOIN LATERAL (
        SELECT open FROM d_timeframe
        WHERE share_id = cr.share_id AND date > (cr.date - INTERVAL '3 month')
        ORDER BY date ASC LIMIT 1
    ) q ON TRUE
    LEFT JOIN LATERAL (
        SELECT open FROM d_timeframe
        WHERE share_id = cr.share_id AND date > (cr.date - INTERVAL '6 month')
        ORDER BY date ASC LIMIT 1
    ) s ON TRUE
    LEFT JOIN LATERAL (
        SELECT open FROM d_timeframe
        WHERE share_id = cr.share_id AND date > (cr.date - INTERVAL '1 year')
        ORDER BY date ASC LIMIT 1
    ) y ON TRUE
),

-- Year-to-date change (uses close, not open, in the original)
year_change AS (
    SELECT cr.share_id, cr.date,
           (cr.close * _magn / NULLIF(yc.close, 0) - 10000) * 100 AS rel_y_change
    FROM current_row cr
    LEFT JOIN LATERAL (
        SELECT close FROM d_timeframe
        WHERE share_id = cr.share_id AND date >= DATE_TRUNC('year', cr.date)
        ORDER BY date ASC LIMIT 1
    ) yc ON TRUE
),

-- All-time low/high
all_time_calc AS (
    SELECT a.share_id, a.date,
           MIN(t.low) AS all_time_low,
           MAX(t.high) AS all_time_high
    FROM affected a
    JOIN LATERAL (
        SELECT low, high FROM d_timeframe
        WHERE share_id = a.share_id AND date <= a.date
    ) t ON TRUE
    GROUP BY a.share_id, a.date
),

-- Combine everything into a single per-row indicator set
combined AS (
    SELECT
        a.share_id, a.date,
        s10.sma10,
        l20.sma20,
        l50.sma50,
        s100.sma100,
        s200.sma200,
        atr.abs_atr,
        CASE WHEN atr.abs_atr IS NOT NULL AND cr.close <> 0
             THEN atr.abs_atr * _magn / cr.close * 100 END AS rel_atr,
        l20.abs_adr,
        l20.rel_adr,
        CASE WHEN cr.volume IS NOT NULL AND cr.vwap IS NOT NULL
             THEN cr.volume * cr.vwap / _magn ELSE 0 END AS dollar_volume,
        l20.avg_volume,
        l20.avg_dollar_volume,
        CASE WHEN l20.avg_volume IS NOT NULL AND l20.avg_volume <> 0
             THEN cr.volume * _magn / l20.avg_volume END AS rel_volume,
        CASE WHEN l40.avg_volume_40 != 0 AND l252.avg_volume_ytd != 0
             THEN l40.avg_volume_40 * _magn / l252.avg_volume_ytd
             ELSE 0 END AS dense_volume,
        (pd.curr_close - pd.prev_close) AS abs_change,
        ((pd.curr_close * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100 AS rel_change,
        ((pd.curr_open  * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100 AS rel_gap,
        CASE
            WHEN ((pd.curr_close * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100 IS NOT NULL
             AND ((pd.curr_open  * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100 IS NOT NULL
            THEN ((pd.curr_close * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100
               - ((pd.curr_open  * _magn / NULLIF(pd.prev_close, 0)) - 10000) * 100
        END AS rel_change_from_open,
        cv.convergence,
        wc.rel_w_change,
        wc.rel_m_change,
        wc.rel_q_change,
        wc.rel_6m_change,
        wc.rel_ytd_change,
        yr.rel_y_change,
        l20.twenty_day_low,
        l20.twenty_day_high,
        l50.fifty_day_low,
        l50.fifty_day_high,
        l252.ytd_low,
        l252.ytd_high,
        at.all_time_low,
        at.all_time_high
    FROM affected a
    JOIN current_row cr      ON cr.share_id   = a.share_id AND cr.date   = a.date
    LEFT JOIN sma10_calc s10 ON s10.share_id  = a.share_id AND s10.date  = a.date
    LEFT JOIN last20_calc l20 ON l20.share_id  = a.share_id AND l20.date  = a.date
    LEFT JOIN last50_calc l50 ON l50.share_id  = a.share_id AND l50.date  = a.date
    LEFT JOIN sma100_calc s100 ON s100.share_id = a.share_id AND s100.date = a.date
    LEFT JOIN sma200_calc s200 ON s200.share_id = a.share_id AND s200.date = a.date
    LEFT JOIN atr_calc atr     ON atr.share_id  = a.share_id AND atr.date  = a.date
    LEFT JOIN last40_calc l40  ON l40.share_id  = a.share_id AND l40.date  = a.date
    LEFT JOIN last252_calc l252 ON l252.share_id = a.share_id AND l252.date = a.date
    LEFT JOIN prev_day_calc pd ON pd.share_id   = a.share_id AND pd.date   = a.date
    LEFT JOIN conv_calc cv     ON cv.share_id   = a.share_id AND cv.date   = a.date
    LEFT JOIN window_changes wc ON wc.share_id  = a.share_id AND wc.date   = a.date
    LEFT JOIN year_change yr   ON yr.share_id   = a.share_id AND yr.date   = a.date
    LEFT JOIN all_time_calc at ON at.share_id   = a.share_id AND at.date   = a.date
)

-- Single UPDATE to write all indicators
UPDATE d_timeframe d
SET sma10                = c.sma10,
    sma20                = c.sma20,
    sma50                = c.sma50,
    sma100               = c.sma100,
    sma200               = c.sma200,
    abs_atr              = c.abs_atr,
    rel_atr              = c.rel_atr,
    abs_adr              = c.abs_adr,
    rel_adr              = c.rel_adr,
    dollar_volume        = c.dollar_volume,
    avg_volume           = c.avg_volume,
    avg_dollar_volume    = c.avg_dollar_volume,
    rel_volume           = c.rel_volume,
    dense_volume         = c.dense_volume,
    abs_change           = c.abs_change,
    rel_change           = c.rel_change,
    rel_gap              = c.rel_gap,
    rel_change_from_open = c.rel_change_from_open,
    convergence          = c.convergence,
    rel_w_change         = c.rel_w_change,
    rel_m_change         = c.rel_m_change,
    rel_q_change         = c.rel_q_change,
    rel_6m_change        = c.rel_6m_change,
    rel_ytd_change       = c.rel_ytd_change,
    rel_y_change         = c.rel_y_change,
    twenty_day_low       = c.twenty_day_low,
    twenty_day_high      = c.twenty_day_high,
    fifty_day_low        = c.fifty_day_low,
    fifty_day_high       = c.fifty_day_high,
    ytd_low              = c.ytd_low,
    ytd_high             = c.ytd_high,
    all_time_low         = c.all_time_low,
    all_time_high        = c.all_time_high
FROM combined c
WHERE d.share_id = c.share_id AND d.date = c.date;

-- ------------------------------------------------------------------------
-- Rollup to w_timeframe
-- ------------------------------------------------------------------------
WITH buckets AS (
    SELECT DISTINCT
        share_id,
        date_trunc('week', date)::date AS bucket_start
    FROM new_rows
),
agg AS (
    SELECT
        b.share_id,
        b.bucket_start AS date,
        (array_agg(t.open  ORDER BY t.date ASC))[1]  AS open,
        MAX(t.high) AS high,
        MIN(t.low)  AS low,
        (array_agg(t.close ORDER BY t.date DESC))[1] AS close,
        SUM(t.volume) AS volume,
        CASE WHEN SUM(t.volume) > 0
             THEN SUM(t.vwap * t.volume) / SUM(t.volume) END AS vwap
    FROM buckets b
    JOIN d_timeframe t
      ON t.share_id = b.share_id
     AND t.date >= b.bucket_start
     AND t.date <  (b.bucket_start + INTERVAL '1 week')::date
    GROUP BY b.share_id, b.bucket_start
)
INSERT INTO w_timeframe (share_id, date, open, high, low, close, volume, vwap)
SELECT share_id, date, open, high, low, close, volume, vwap
FROM agg WHERE close IS NOT NULL
ON CONFLICT (share_id, date) DO UPDATE SET
    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
    volume=EXCLUDED.volume, vwap=EXCLUDED.vwap;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_d_timeframe_trigger     ON d_timeframe;
DROP TRIGGER IF EXISTS update_d_timeframe_ins_trigger ON d_timeframe;
DROP TRIGGER IF EXISTS update_d_timeframe_upd_trigger ON d_timeframe;

CREATE TRIGGER update_d_timeframe_ins_trigger
AFTER INSERT ON d_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_d_timeframe();

CREATE TRIGGER update_d_timeframe_upd_trigger
AFTER UPDATE ON d_timeframe
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_d_timeframe();