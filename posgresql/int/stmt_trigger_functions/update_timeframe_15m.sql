CREATE TRIGGER update_timeframe_15m_insert_trigger
AFTER INSERT
ON timeframe_15m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_15m();
-- Update trigger
CREATE TRIGGER update_timeframe_15m_update_trigger
AFTER UPDATE ON timeframe_15m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_15m();