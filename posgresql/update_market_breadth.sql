CREATE OR REPLACE FUNCTION update_market_breadth()
RETURNS TRIGGER AS $$
DECLARE
    --NEW
    _four_up market_breadth.fourUp%type;
    _four_down market_breadth.four_down%type;
    _four_ratio market_breadth.four_ratio%type;
BEGIN
SELECT
