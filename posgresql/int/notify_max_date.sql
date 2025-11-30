CREATE OR REPLACE FUNCTION notify_max_date()
RETURNS TRIGGER AS $$
DECLARE
    _new_max_date DATE;
    _old_max_date DATE;
BEGIN
    SELECT MAX(date) INTO _new_max_date FROM d_timeframe;
    SELECT max_date INTO _old_max_date FROM config_values WHERE id = 1;

    -- Send notification only for new max date
    IF _old_max_date IS NULL OR _new_max_date > _old_max_date THEN
        -- Update stored state
        UPDATE config_values SET max_date = _new_max_date WHERE id = 1;

        -- Notify App
        PERFORM pg_notify('max_date_channel', _new_max_date::text);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_max_date_trigger
AFTER INSERT OR UPDATE ON d_timeframe
FOR EACH STATEMENT
EXECUTE FUNCTION notify_max_date();

