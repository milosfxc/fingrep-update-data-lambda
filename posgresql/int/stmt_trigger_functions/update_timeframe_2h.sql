CREATE TRIGGER update_timeframe_2h_insert_trigger
AFTER INSERT
ON timeframe_2h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2h();
-- Update trigger
CREATE TRIGGER update_timeframe_2h_update_trigger
AFTER UPDATE ON timeframe_2h
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_2h();