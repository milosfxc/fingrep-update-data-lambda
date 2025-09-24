--BALANCE SHEET
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_balance_sheet()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM balance_sheet WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.fiscal_period_id := NULL;
    END IF;
	IF EXISTS (SELECT 1 FROM balance_sheet WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_balance_sheet
BEFORE INSERT ON balance_sheet
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_balance_sheet();

--INCOME STATEMENT
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_income_statement()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM income_statement WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.fiscal_period_id := NULL;
    END IF;
	IF EXISTS (SELECT 1 FROM income_statement WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_income_statement
BEFORE INSERT ON income_statement
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_income_statement();

--CASHFLOW STATEMENT
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_cash_flow_statement()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM cash_flow_statement WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.fiscal_period_id := NULL;
    END IF;
	IF EXISTS (SELECT 1 FROM cash_flow_statement WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id)
	AND NOT EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type)
	THEN
        NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_cash_flow_statement
BEFORE INSERT ON cash_flow_statement
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_cash_flow_statement();


--INCOME STATEMENT NEW
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_income_statement()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM income_statement WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id) AND NOT EXISTS (SELECT 1 FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND fiscal_period_id = NEW.fiscal_period_id) THEN
            NEW.fiscal_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM income_statement WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id) THEN
            NEW.fiscal_period_id := NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type) AND NOT EXISTS (SELECT 1 FROM income_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND calendar_period_id = NEW.calendar_period_id) THEN
            NEW.calendar_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM income_statement WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id) THEN
            NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_income_statement
BEFORE INSERT ON income_statement
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_income_statement();

--BALANCE SHEET NEW
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_balance_sheet()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM balance_sheet WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id) AND NOT EXISTS (SELECT 1 FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND fiscal_period_id = NEW.fiscal_period_id) THEN
            NEW.fiscal_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM balance_sheet WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id) THEN
            NEW.fiscal_period_id := NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type) AND NOT EXISTS (SELECT 1 FROM balance_sheet WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND calendar_period_id = NEW.calendar_period_id) THEN
            NEW.calendar_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM balance_sheet WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id) THEN
            NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_balance_sheet
BEFORE INSERT ON balance_sheet
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_balance_sheet();


--CASHFLOW STATEMENT NEW
CREATE OR REPLACE FUNCTION handle_fiscal_and_calendar_period_conflict_cash_flow_statement()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type) AND NOT EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND fiscal_period_id = NEW.fiscal_period_id) THEN
            NEW.fiscal_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM cash_flow_statement WHERE fiscal_period_id = NEW.fiscal_period_id AND share_id = NEW.share_id) THEN
            NEW.fiscal_period_id := NULL;
    END IF;
    IF EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type) AND NOT EXISTS (SELECT 1 FROM cash_flow_statement WHERE share_id = NEW.share_id AND date = NEW.date AND report_type = NEW.report_type AND calendar_period_id = NEW.calendar_period_id) THEN
            NEW.calendar_period_id := NULL;
    ELSIF EXISTS (SELECT 1 FROM cash_flow_statement WHERE calendar_period_id = NEW.calendar_period_id AND share_id = NEW.share_id) THEN
            NEW.calendar_period_id := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_fiscal_and_calendar_conflict_cash_flow_statement
BEFORE INSERT ON cash_flow_statement
FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_and_calendar_period_conflict_cash_flow_statement();