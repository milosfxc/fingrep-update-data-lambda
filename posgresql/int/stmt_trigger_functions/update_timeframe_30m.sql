CREATE TRIGGER update_timeframe_30m_insert_trigger
AFTER INSERT
ON timeframe_30m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_30m();
-- Update trigger
CREATE TRIGGER update_timeframe_30m_update_trigger
AFTER UPDATE ON timeframe_30m
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
WHEN (pg_trigger_depth() = 0)
EXECUTE FUNCTION update_timeframe_30m();