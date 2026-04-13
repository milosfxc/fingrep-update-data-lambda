CREATE OR REPLACE FUNCTION update_timeframe_1m_bulk()
RETURNS TRIGGER AS $$
DECLARE
    _magn BIGINT := 10000;
    _sma10 BIGINT;
    _abs_atr BIGINT;
    _avg_volume BIGINT;
    _rel_volume BIGINT;
    _convergence2 BOOLEAN;
    _convergence3 BOOLEAN;
    _timeframe_2m_start TIMESTAMP;
    _timeframe_2m_end TIMESTAMP;
    _timeframe_3m_start TIMESTAMP;
    _timeframe_3m_end TIMESTAMP;
    _timeframe_5m_start TIMESTAMP;
    _timeframe_5m_end TIMESTAMP;
    _open BIGINT;
    _close BIGINT;
    _high BIGINT;
    _low BIGINT;
    _volume BIGINT;
    _vwap BIGINT;
    _session SMALLINT;
    _rec RECORD;   -- ← holds each inserted row
BEGIN

-- Loop over every inserted row (transition table)
FOR _rec IN SELECT * FROM inserted_rows LOOP

    -- SMA10
    WITH last_10 AS (
        SELECT close FROM timeframe_1m
        WHERE share_id = _rec.share_id AND datetime <= _rec.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 10
    )
    SELECT CASE WHEN COUNT(*) = 10 THEN AVG(close) END INTO _sma10 FROM last_10;

    -- ABS ATR
    WITH last_14 AS (
        SELECT high - low AS high_low,
               ABS(high - LAG(close) OVER (ORDER BY datetime)) AS high_prev_close,
               ABS(low  - LAG(close) OVER (ORDER BY datetime)) AS low_prev_close
        FROM timeframe_1m
        WHERE share_id = _rec.share_id AND datetime <= _rec.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 14
    )
    SELECT CASE WHEN COUNT(*) = 14 THEN AVG(GREATEST(high_low, high_prev_close, low_prev_close)) END
    INTO _abs_atr FROM last_14;

    -- AVG VOL
    WITH last_20 AS (
        SELECT volume FROM timeframe_1m
        WHERE share_id = _rec.share_id AND datetime <= _rec.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 20
    )
    SELECT CASE WHEN COUNT(*) = 20 THEN AVG(volume) END INTO _avg_volume FROM last_20;

    -- RVOL
    _rel_volume := NULL;
    IF _avg_volume > 0 AND _rec.volume > 0 THEN
        _rel_volume := _rec.volume * _magn / _avg_volume;
    END IF;

    -- CONVERGENCE 2
    WITH last_2 AS (
        SELECT datetime, high, low FROM timeframe_1m
        WHERE share_id = _rec.share_id AND datetime <= _rec.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 2
    )
    SELECT CASE WHEN COUNT(*) = 2
                 AND MAX(high) = (SELECT high FROM last_2 ORDER BY datetime ASC LIMIT 1)
                 AND MIN(low)  = (SELECT low  FROM last_2 ORDER BY datetime ASC LIMIT 1)
           THEN true ELSE false END INTO _convergence2 FROM last_2;

    -- CONVERGENCE 3
    WITH last_3 AS (
        SELECT datetime, high, low FROM timeframe_1m
        WHERE share_id = _rec.share_id AND datetime <= _rec.datetime AND session = 1
        ORDER BY datetime DESC LIMIT 3
    )
    SELECT CASE WHEN COUNT(*) = 3
                 AND MAX(high) = (SELECT high FROM last_3 ORDER BY datetime ASC LIMIT 1)
                 AND MIN(low)  = (SELECT low  FROM last_3 ORDER BY datetime ASC LIMIT 1)
           THEN true ELSE false END INTO _convergence3 FROM last_3;

    -- UPDATE 1m row
    UPDATE timeframe_1m SET
        sma10        = _sma10,
        abs_atr      = _abs_atr,
        convergence2 = _convergence2,
        convergence3 = _convergence3,
        avg_volume   = _avg_volume,
        rel_volume   = _rel_volume,
        time = CAST(to_char(_rec.datetime, 'HH24MI') AS smallint);
    WHERE share_id = _rec.share_id AND datetime = _rec.datetime;

    -- TIMEFRAME 2m
    _timeframe_2m_start := timeframe_min_start(_rec.datetime, 2);
    _timeframe_2m_end   := _timeframe_2m_start + INTERVAL '2 minutes';
    _close := NULL; _session := NULL;

    SELECT (ARRAY_AGG(open  ORDER BY datetime ASC))[1], MAX(high), MIN(low),
           (ARRAY_AGG(close ORDER BY datetime DESC))[1], SUM(volume),
           CASE WHEN SUM(volume) > 0 THEN SUM(vwap * volume) / SUM(volume) END,
           (ARRAY_AGG(session ORDER BY datetime ASC))[1]
    INTO _open, _high, _low, _close, _volume, _vwap, _session
    FROM timeframe_1m
    WHERE share_id = _rec.share_id AND datetime >= _timeframe_2m_start AND datetime < _timeframe_2m_end;

    IF _close > 0 AND _session IS NOT NULL THEN
        INSERT INTO timeframe_2m (share_id, datetime, open, high, low, close, volume, vwap, session)
        VALUES (_rec.share_id, _timeframe_2m_start, _open, _high, _low, _close, _volume, _vwap, _session)
        ON CONFLICT (share_id, datetime) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, session = EXCLUDED.session;
    END IF;

    -- TIMEFRAME 3m
    _timeframe_3m_start := timeframe_min_start(_rec.datetime, 3);
    _timeframe_3m_end   := _timeframe_3m_start + INTERVAL '3 minutes';
    _close := NULL; _session := NULL;

    SELECT (ARRAY_AGG(open  ORDER BY datetime ASC))[1], MAX(high), MIN(low),
           (ARRAY_AGG(close ORDER BY datetime DESC))[1], SUM(volume),
           CASE WHEN SUM(volume) > 0 THEN SUM(vwap * volume) / SUM(volume) END,
           (ARRAY_AGG(session ORDER BY datetime ASC))[1]
    INTO _open, _high, _low, _close, _volume, _vwap, _session
    FROM timeframe_1m
    WHERE share_id = _rec.share_id AND datetime >= _timeframe_3m_start AND datetime < _timeframe_3m_end;

    IF _close > 0 AND _session IS NOT NULL THEN
        INSERT INTO timeframe_3m (share_id, datetime, open, high, low, close, volume, vwap, session)
        VALUES (_rec.share_id, _timeframe_3m_start, _open, _high, _low, _close, _volume, _vwap, _session)
        ON CONFLICT (share_id, datetime) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, session = EXCLUDED.session;
    END IF;

    -- TIMEFRAME 5m
    _timeframe_5m_start := timeframe_min_start(_rec.datetime, 5);
    _timeframe_5m_end   := _timeframe_5m_start + INTERVAL '5 minutes';
    _close := NULL; _session := NULL;

    SELECT (ARRAY_AGG(open  ORDER BY datetime ASC))[1], MAX(high), MIN(low),
           (ARRAY_AGG(close ORDER BY datetime DESC))[1], SUM(volume),
           CASE WHEN SUM(volume) > 0 THEN SUM(vwap * volume) / SUM(volume) END,
           (ARRAY_AGG(session ORDER BY datetime ASC))[1]
    INTO _open, _high, _low, _close, _volume, _vwap, _session
    FROM timeframe_1m
    WHERE share_id = _rec.share_id AND datetime >= _timeframe_5m_start AND datetime < _timeframe_5m_end;

    IF _close > 0 AND _session IS NOT NULL THEN
        INSERT INTO timeframe_5m (share_id, datetime, open, high, low, close, volume, vwap, session)
        VALUES (_rec.share_id, _timeframe_5m_start, _open, _high, _low, _close, _volume, _vwap, _session)
        ON CONFLICT (share_id, datetime) DO UPDATE SET
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
            close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, session = EXCLUDED.session;
    END IF;

END LOOP;

RETURN NULL;  -- statement-level triggers must return NULL
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timeframe_1m_bulk_trigger
AFTER INSERT ON timeframe_1m
REFERENCING NEW TABLE AS inserted_rows   -- transition table
FOR EACH STATEMENT                        -- fires once per statement
EXECUTE FUNCTION update_timeframe_1m_bulk();