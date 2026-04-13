CREATE OR REPLACE PROCEDURE refresh_d_timeframe(p_share_ids INTEGER[] DEFAULT NULL)
LANGUAGE plpgsql AS $$
BEGIN

    WITH base0 AS (
        SELECT
            share_id, date, open, high, low, close, volume, vwap,
            LAG(close) OVER (PARTITION BY share_id ORDER BY date) AS prev_close
        FROM d_timeframe
        WHERE (p_share_ids IS NULL OR share_id = ANY(p_share_ids))
    ),

    base AS (
        SELECT
            share_id, date, open, high, low, close, volume, vwap,
            prev_close,

            -- True range components
            (high - low) AS high_low,
            ABS(high - prev_close) AS high_prev_close,
            ABS(low  - prev_close) AS low_prev_close,

            -- SMA20
            CASE WHEN COUNT(*) OVER w20 = 20
                 THEN AVG(close) OVER w20
            END AS sma20,

            -- ATR
            CASE WHEN COUNT(*) OVER w14 = 14 THEN
                AVG(GREATEST(
                    (high - low),
                    ABS(high - prev_close),
                    ABS(low  - prev_close)
                )) OVER w14
            END AS abs_atr,

            -- ADR & averages
            CASE WHEN COUNT(*) OVER w20 = 20 THEN
                100 * (AVG(high * 10000.0 / NULLIF(low,0)) OVER w20 - 10000)
            END AS rel_adr,

            CASE WHEN COUNT(*) OVER w20 = 20
                 THEN AVG(high - low) OVER w20
            END AS abs_adr,

            CASE WHEN COUNT(*) OVER w20 = 20
                 THEN AVG(volume) OVER w20
            END AS avg_volume,

            -- Changes
            close - prev_close AS abs_change,
            open AS open_today

        FROM base0

        WINDOW
            w14 AS (PARTITION BY share_id ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
            w20 AS (PARTITION BY share_id ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    )

    UPDATE d_timeframe t
    SET
        sma20      = b.sma20,
        abs_atr    = b.abs_atr,

        rel_atr    = CASE
                        WHEN b.abs_atr IS NOT NULL AND b.close <> 0
                        THEN (b.abs_atr * 10000 / b.close) * 100
                     END,

        abs_adr    = b.abs_adr,
        rel_adr    = b.rel_adr,
        avg_volume = b.avg_volume,

        rel_volume = CASE
                        WHEN b.avg_volume <> 0
                        THEN (b.volume * 10000 / b.avg_volume)
                     END,

        abs_change = b.abs_change,

        rel_change = CASE
                        WHEN b.prev_close <> 0
                        THEN ((b.close * 10000 / b.prev_close) - 10000) * 100
                     END,

        rel_gap = CASE
                    WHEN b.prev_close <> 0
                    THEN ((b.open_today * 10000 / b.prev_close) - 10000) * 100
                  END,

        rel_change_from_open =
            CASE
                WHEN b.prev_close <> 0
                THEN (
                    ((b.close * 10000 / b.prev_close) - 10000) * 100
                  - ((b.open_today * 10000 / b.prev_close) - 10000) * 100
                )
            END

    FROM base b
    WHERE t.share_id = b.share_id
      AND t.date     = b.date
      AND t.date >= CURRENT_DATE - INTERVAL '7 days';

    -- =========================
    -- WEEKLY AGGREGATION
    -- =========================
    INSERT INTO w_timeframe (
        share_id, date, open, high, low, close, volume, vwap
    )
    SELECT
        share_id,
        date_trunc('week', date)::date AS week_start,

        (ARRAY_AGG(open ORDER BY date ASC))[1],
        MAX(high),
        MIN(low),
        (ARRAY_AGG(close ORDER BY date DESC))[1],
        SUM(volume),

        CASE
            WHEN SUM(volume) > 0
            THEN SUM(vwap * volume) / SUM(volume)
        END

    FROM d_timeframe
    WHERE (p_share_ids IS NULL OR share_id = ANY(p_share_ids))
      AND date >= CURRENT_DATE - INTERVAL '7 days'

    GROUP BY share_id, date_trunc('week', date)

    HAVING (ARRAY_AGG(close ORDER BY date DESC))[1] IS NOT NULL

    ON CONFLICT (share_id, date) DO UPDATE
    SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume,
        vwap   = EXCLUDED.vwap;

END;
$$;