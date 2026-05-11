-- =========================================
-- timeframe_3m
-- =========================================
CREATE OR REPLACE FUNCTION update_timeframe_3m()
RETURNS TRIGGER AS $$
BEGIN

WITH affected AS (
    SELECT DISTINCT share_id, datetime
    FROM new_rows
),
sma_calc AS (
    SELECT
        a.share_id,
        a.datetime,
        CASE WHEN COUNT(t.close) = 10 THEN AVG(t.close) END AS sma10
    FROM affected a
    JOIN LATERAL (
        SELECT close
        FROM timeframe_3m
        WHERE share_id = a.share_id
          AND datetime <= a.datetime
          AND session = 1
        ORDER BY datetime DESC
        LIMIT 10
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
),
atr_calc AS (
    SELECT
        a.share_id,
        a.datetime,
        CASE WHEN COUNT(*) FILTER (WHERE prev_close IS NOT NULL) = 14
             THEN AVG(GREATEST(high - low,
                               ABS(high - prev_close),
                               ABS(low  - prev_close)))
             END AS abs_atr
    FROM affected a
    JOIN LATERAL (
        SELECT
            high, low,
            LAG(close) OVER (ORDER BY datetime) AS prev_close
        FROM (
            SELECT datetime, high, low, close
            FROM timeframe_3m
            WHERE share_id = a.share_id
              AND datetime <= a.datetime
              AND session = 1
            ORDER BY datetime DESC
            LIMIT 15
        ) last15
        ORDER BY datetime ASC
    ) t ON TRUE
    GROUP BY a.share_id, a.datetime
)
UPDATE timeframe_3m tf
SET sma10   = s.sma10,
    abs_atr = atr.abs_atr
FROM sma_calc s
JOIN atr_calc atr USING (share_id, datetime)
WHERE tf.share_id = s.share_id
  AND tf.datetime = s.datetime;

RETURN NULL;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS update_timeframe_3m_trigger     ON timeframe_3m;
DROP TRIGGER IF EXISTS update_timeframe_3m_ins_trigger ON timeframe_3m;
DROP TRIGGER IF EXISTS update_timeframe_3m_upd_trigger ON timeframe_3m;

CREATE TRIGGER update_timeframe_3m_ins_trigger
AFTER INSERT ON timeframe_3m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3m();

CREATE TRIGGER update_timeframe_3m_upd_trigger
AFTER UPDATE ON timeframe_3m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_3m();