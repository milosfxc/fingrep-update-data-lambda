from datetime import timedelta, datetime, date
from typing import Optional, Dict, Union
import edgar
import numpy as np
import pandas as pd
from edgar import Filing,set_identity
import XBRLTagMapper
import config
import db_ops
from config import logger
import utils


def get_filings_by_company(ticker: str, cutoff_date:str) -> pd.DataFrame:
    """
    Functions returns dataframe of all filings by company.
    :param ticker:
    :param cutoff_date: Filing's report date must be equal or bigger than cut-off date.
    :return:
    """
    # Filters for the query below
    forms = ['10-K','10-Q','20-F']
    cutoff_date = pd.to_datetime(cutoff_date)
    df_filings = (edgar.Company(ticker).get_filings().to_pandas()
                  .assign(reportDate=lambda x: pd.to_datetime(x['reportDate']))
                  .query('reportDate >= @cutoff_date and form in @forms')
                  .sort_values('reportDate', ascending=True)
                  .reset_index(drop=True))
    if not df_filings.empty and {'accession_number', 'reportDate','form', 'isXBRL', 'acceptanceDateTime'}.issubset(set(df_filings.columns)):
        return df_filings[['accession_number', 'reportDate','form', 'isXBRL', 'acceptanceDateTime']]
    else:
        logger.info(f"Couldn't obtain a list of filings for ticker {ticker}. Dataframe df_filings:\n{df_filings}")
        return pd.DataFrame(data=None)

def get_latest_filings(filing_date:str) -> pd.DataFrame:
    """
    Functions returns a dataframe of all filings by company.
    :param filing_date: Filing start date
    :return: Dataframe
    """

    # Filters for the query below
    forms = list(utils.forms['annual'].union(utils.forms['quarterly']))
    df_filings = (edgar.get_filings(filing_date=filing_date, amendments=False).to_pandas()
                  .query('form in @forms'))
    if not df_filings.empty and {'form', 'cik', 'filing_date', 'accession_number'}.issubset(set(df_filings.columns)):
        return df_filings
    else:
        logger.warning(f"Couldn't obtain a list of latest filings or list didn't had all required columns for filing_date {filing_date}. Dataframe df_filings: \n{df_filings}")
        return pd.DataFrame(data=None)


def get_filing_details(accession_number:str, is_xbrl:int, share_id: int, filing:Filing = None) -> Dict[str,dict] | None:
    if  is_xbrl == 1:
        filing = edgar.get_by_accession_number(accession_number=accession_number) if filing is None else filing
        # Ratio triggers require this order IS -> CFS -> BS
        if not filing:
            logger.warning(f"Filing with accession number {accession_number} was None")
            return None
        try:
            financials = filing.obj().financials
        except AttributeError:
            logger.warning(f"Filing {accession_number} missing attributes")
            return None

        statements = {
            'IncomeStatement': financials.income_statement().to_dataframe().replace(['',np.nan],None) if financials.income_statement() else pd.DataFrame(),
            'CashFlowStatement': financials.cashflow_statement().to_dataframe().replace(['',np.nan],None) if financials.cashflow_statement() else pd.DataFrame(),
            'BalanceSheet': financials.balance_sheet().to_dataframe().replace(['',np.nan],None) if financials.balance_sheet() else pd.DataFrame()
        }
        # Rename columns to match period end date
        for key,stmt in statements.items():
            if len(stmt.columns) > 2 and filing.period_of_report in stmt.columns[2] and {'concept', 'label'}.issubset(stmt.columns):
                stmt = stmt[['concept', 'label', stmt.columns[2]]]
                statements[key] = stmt.rename(columns={stmt.columns[2]:filing.period_of_report}, inplace=False)
            else:
                statements[key] = get_statement_by_xbrl_query(filing,key)
                if statements[key].empty:
                    logger.warning(f"Could not find {key} in filing with accession number: {filing.accession_number}")
                    return {}
        # Other data
        other_data = get_other_data(filing, statements['IncomeStatement'], statements['CashFlowStatement'])
        if not other_data: return None
        other_data['share_id'] = share_id
        # Dataframes for XBRL query
        previous_end_date = (datetime.strptime(other_data['period_start'], '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        df_instant_prev_end = filing.xbrl().query().by_dimension(None).by_instant_date(previous_end_date).to_dataframe('concept', 'numeric_value', 'statement_type').replace(['',np.nan],None)
        df_instant_end = filing.xbrl().query().by_dimension(None).by_instant_date(filing.period_of_report).to_dataframe('concept', 'numeric_value', 'statement_type').replace(['',np.nan],None)
        df_period = filing.xbrl().query().by_dimension(None).by_date_range(other_data['period_start'], filing.period_of_report).to_dataframe('concept', 'numeric_value', 'statement_type').replace(['',np.nan],None)
        # Archive filing
        archive = {
            'period_start': other_data['period_start'],
            'period_end': filing.period_of_report,
            'cf_period_start': other_data['cf_period_start'],
            'acc_standard': other_data['acc_standard'],
            'entity_info': {k: v.strftime('%Y-%m-%d') if isinstance(v, (date,datetime)) else v for k,v in filing.xbrl().entity_info.items()},
            'currency': other_data['currency'],
            'form': filing.form,
            'cik': filing.cik,
            'statements': {key: df.to_dict() for key, df in statements.items()},
            'df_instant_prev_end': df_instant_prev_end.to_dict(),
            'df_instant_end': df_instant_end.to_dict(),
            'df_period': df_period.to_dict()
        }
        db_ops.insert_filing(filing.accession_number, archive)
        # # XBRL Mappings
        # xbrl_map = dict()
        # # Load data from pickle
        # if os.path.exists('data/xbrl_map'):
        #     with open('data/xbrl_map', 'rb') as f:
        #         xbrl_map = pickle.load(f)
        for stmt in statements.keys():
            statements[stmt] = get_statement(stmt, statements[stmt], other_data, df_instant_end, df_period, filing.period_of_report)
        # Income statement and cashflow statement period compatibility
        if (cf_period_start := other_data.get('cf_period_start')) != (period_start := other_data.get('period_start')):
            cf_stmt = get_quarterly_cash_flow_statement(statements.pop('CashFlowStatement'),share_id, cf_period_start, period_start)
            if cf_stmt:
                statements['CashFlowStatement'] = cf_stmt

        # Data validation
        statements['BalanceSheet'] = validate_balance_sheet(statements['BalanceSheet'])
        statements['IncomeStatement'] = validate_income_statement(inc_stmt=statements['IncomeStatement'],cf_stmt=statements.get('CashFlowStatement'))
        if 'CashFlowStatement' in statements.keys(): # To prevent KeyError if IncomeStatement and CashFlowStatement period compatibility Fails
            statements['CashFlowStatement'] = validate_cashflow_statement(df_instant_prev_end,df_instant_end,df_period,statements['CashFlowStatement'], other_data['acc_standard'])
        return statements
    else:
        return None


def get_statement(stmt_name: str, df_stmt: pd.DataFrame, other_data: dict, df_instant: pd.DataFrame, df_period: pd.DataFrame, period_end: str) -> dict[str,float]:
    stmt_tags = XBRLTagMapper.xbrl_tags[other_data['acc_standard']][stmt_name].copy()
    df_period = df_period[df_period['statement_type'] != 'CashFlowStatement'] if stmt_name == 'IncomeStatement' else df_period[df_period['statement_type'] != 'IncomeStatement']
    df_tags = df_instant if stmt_name == 'BalanceSheet' else df_period
    # Replace _ with:
    df_stmt['concept'] = df_stmt['concept'].str.replace('_', ':')
    ans = {}
    # Income statement special positions
    if stmt_name == 'IncomeStatement':
        # revenue_series = df_stmt.loc[ todo check this error str >= float
        # df_stmt['label'].str.contains('revenue', case=False, na=False) & df_stmt[period_end].notna(), period_end]
        # ans['revenue'] = revenue_series.max() * config.scale_factor if not revenue_series.empty else None
        # Revenue
        ans['revenue'] = get_position_value_sum_or_max(df_stmt,df_tags,stmt_tags.pop('revenue'),period_end)
        if not ans['revenue']:
            revenue_series = pd.to_numeric(df_stmt.loc[df_stmt['label'].str.contains('revenue', case=False, na=False) & df_stmt[period_end].notna(), period_end],errors='coerce')
            ans['revenue'] = revenue_series.max() * config.scale_factor if not revenue_series.empty else None
        # Net Income
        ans['net_income'] = get_position_value_sum_or_max(df_stmt,df_tags,stmt_tags.pop('net_income'),period_end)
    # Iterate over statement positions
    for position, tags in stmt_tags.items():
        if tags is not None:
            ans[position] = get_position_value_sum_or_sum(df_stmt, df_tags,tags, period_end)
        else:
            ans[position] = None
    # Additional data
    ans['fiscal_period_id'] = utils.report_periods.get(other_data.get('fiscal_period'))
    ans['calendar_period_id'] = utils.report_periods.get(other_data.get('calendar_period'))
    ans['currency_id'] = db_ops.get_cached_foreign_keys()['currencies'].get(other_data.get('currency'))
    ans['date'] = other_data['date']
    ans['filing_date'] = other_data.get('filing_date')
    ans['report_type'] = other_data.get('report_type')
    ans['share_id'] = other_data['share_id']
    return ans


def get_position_value_sum_or_sum(df_stmt: pd.DataFrame, df_tags: pd.DataFrame, xbrl_tags: set, period_end: str) -> float | None:
    df_stmt = df_stmt[df_stmt['concept'].isin(xbrl_tags)]
    values = pd.to_numeric(df_stmt[period_end], errors='coerce').dropna().tolist()
    # EXTRACTED XBRL INSTANCE DOCUMENT if statement has no values for position
    if not values:
        df_tags = df_tags[df_tags['concept'].isin(xbrl_tags)]
        tag_values = pd.to_numeric(df_tags['numeric_value'], errors='coerce').dropna().tolist()
        if tag_values:
            values.append(max(tag_values, key=abs))
        if not values:
            return None
    max_value, sum_value, sum_combo = max(values), sum(values),check_sum_combinations(values)
    pos_value = max_value if sum_value - max_value == max_value else sum_combo if sum_combo else sum_value
    return pos_value * config.scale_factor if pos_value != 0 else None


def get_position_value_sum_or_max(df_stmt: pd.DataFrame, df_tags: pd.DataFrame, xbrl_tags: set, period_end: str) -> float | None:
    df_stmt = df_stmt[df_stmt['concept'].isin(xbrl_tags)]
    values = pd.to_numeric(df_stmt[period_end], errors='coerce').dropna().tolist()
    # EXTRACTED XBRL INSTANCE DOCUMENT if statement has no values for position
    if not values:
        df_tags = df_tags[df_tags['concept'].isin(xbrl_tags)]
        tag_values = pd.to_numeric(df_tags['numeric_value'], errors='coerce').dropna().tolist()
        if tag_values:
            values.append(max(tag_values, key=abs))
        if not values:
            return None
    max_value, sum_value = max(values), sum(values)
    pos_value = max_value if sum_value - max_value == max_value else max_value
    return pos_value * config.scale_factor if pos_value != 0 else None


def get_other_data(filing: edgar.Filing, df_inc: pd.DataFrame, df_cf) -> dict | None:
    """
    :param filing:
    :param df_inc:
    :param df_cf:
    :return: repot_type, acc_standard, currency, period_start, filing_date, date, fiscal_period, calendar_period, cf_period_start
    """
    ans = {}
    # Report type
    if filing.form in utils.forms['annual']:
        ans['report_type'] = 'a'
    elif filing.form in utils.forms['quarterly']:
        ans['report_type'] = 'q'

    # Numeric column values
    values_inc = pd.to_numeric(df_inc[filing.period_of_report], errors='coerce').dropna()
    values_cf = pd.to_numeric(df_cf[filing.period_of_report], errors='coerce').dropna()
    # Accounting standard
    for concept in df_inc['concept']:
        if concept.startswith('us-gaap'):
            ans['acc_standard'] = 'us-gaap'
            break
        elif concept.startswith('ifrs-full'):
            ans['acc_standard'] = 'ifrs-full'
            break
        else:
            ans['acc_standard'] = None
    # Currency and period start

    for value in values_inc[:100]:
        if all(key in ans for key in ['currency', 'period_start']):
            break
        df_pos = filing.xbrl().query().by_statement_type('IncomeStatement').by_dimension(None).by_value(
            float(value)).to_dataframe('concept', 'period_start', 'period_end', 'unit_ref').drop_duplicates()
        if len(df_pos) == 1 and df_pos.loc[0, 'period_end'] == filing.period_of_report:
            ans['period_start'] = df_pos.loc[0, 'period_start']
            currency = get_currency(filing, df_pos.loc[0, 'unit_ref'])
            if currency:
                ans['currency'] = currency
    # Filing date and period of report
    ans['filing_date'] = filing.header.acceptance_datetime
    ans['date'] = filing.period_of_report
    # Fiscal and Calendar Period
    entity_info = filing.xbrl().entity_info
    ans['fiscal_period'] = get_fiscal_period(entity_info,filing.form)
    ans['calendar_period'] = get_calendar_period(filing.period_of_report, filing.form)
    # Period start for cashflow
    for value in values_cf[:25]:
        if all(key in ans for key in ['currency', 'period_start', 'cf_period_start']):
            return ans
        df_pos = (filing.xbrl().query().by_statement_type('CashFlowStatement').by_dimension(None).by_value(float(value))
                  .to_dataframe('period_start', 'period_end').drop_duplicates())
        if len(df_pos) == 1 and {'period_start', 'period_end'}.issubset(df_pos.columns) and df_pos.loc[0, 'period_end'] == filing.period_of_report:
            ans['cf_period_start'] = df_pos.loc[0, 'period_start']
    # todo Check if you can reduce conditions for None, maybe I don't need all ans keys.
    for k in ['report_type', 'acc_standard', 'currency', 'period_start', 'filing_date', 'date', 'fiscal_period', 'calendar_period', 'cf_period_start']:
        if k not in ans:
            ans[k] = None
    logger.warning(f"get_other_data returned None for filing {filing.accession_number}, although it has collected this data:\n {ans}.")
    return None


def get_currency(filing: edgar.Filing, unit_ref: str) -> str | None:
    iso_cur = filing.xbrl().units[unit_ref]['measure'].split(':')
    if len(iso_cur) == 2 and 'iso4217' in iso_cur[0].lower() and len(iso_cur[1]) == 3:
        return iso_cur[1].upper()
    return None


def check_sum_combinations(values: list[float]):
    sum_val = sum(values)
    for val in values:
        if val == (sum_val - abs(val)):
            return val
    return None


def validate_balance_sheet(bs_stmt: dict[str,float]) -> dict[str,float] | None:

    assets = bs_stmt.get('assets') or 0
    liab_and_equity = bs_stmt.pop('liabilities_and_equity') or 0
    assets = max(assets, liab_and_equity)
    assets = assets if assets > 0 else None
    property_plant_equipment_net = bs_stmt.get('property_plant_equipment_net') or 0
    operating_lease = bs_stmt.pop('operating_lease') or 0
    current_assets = bs_stmt.get('current_assets')
    non_current_assets = bs_stmt.get('non_current_assets')
    liabilities = bs_stmt.get('liabilities')
    current_liabilities = bs_stmt.get('current_liabilities')
    non_current_liabilities = bs_stmt.get('non_current_liabilities')
    debt = bs_stmt['debt']
    equity = bs_stmt.get('equity')
    shareholders_equity = bs_stmt.get('shareholders_equity')
    if not shareholders_equity and not equity:
        sh_eq_pos = [bs_stmt.get('capital_stock'), bs_stmt.get('share_premium'), bs_stmt.get('retained_earnings'), bs_stmt.get('treasury_shares'), bs_stmt.get('accumulated_other_comprehensive_income_loss')]
        shareholders_equity = sum(pos for pos in sh_eq_pos if pos)
    equity = equity if equity else shareholders_equity

    # left side
    if assets:
        if not current_assets:
            current_assets = assets - non_current_assets if non_current_assets else None
        if not non_current_assets:
            non_current_assets = assets - current_assets if current_assets else None
        if not liabilities:
            if current_liabilities and non_current_liabilities:
                liabilities = current_liabilities + non_current_liabilities
            elif equity:
                liabilities = assets - equity
        if liabilities:
            equity = assets - liabilities
    # right side
    if liabilities:
        if not current_liabilities:
            current_liabilities = liabilities - non_current_liabilities if non_current_liabilities else None
        if not non_current_liabilities:
            non_current_liabilities = liabilities - current_liabilities if current_liabilities else None
    else:
        if current_liabilities and non_current_liabilities:
            liabilities = current_liabilities + non_current_liabilities
    # Update major balance sheet positions
    bs_stmt['current_assets'] = current_assets
    bs_stmt['non_current_assets'] = non_current_assets
    bs_stmt['liabilities'] = liabilities
    bs_stmt['current_liabilities'] = current_liabilities
    bs_stmt['non_current_liabilities'] = non_current_liabilities
    bs_stmt['equity'] = equity
    # Calc complex position
    # cash_and_short_term_investments
    cash_and_short_term_investments = bs_stmt.get('cash_and_short_term_investments')
    if not cash_and_short_term_investments:
        cash_and_cash_equivalents = bs_stmt.get('cash_and_cash_equivalents') or 0
        short_term_investments = bs_stmt.get('short_term_investments') or 0
        if cash_and_cash_equivalents or short_term_investments:
            cash_and_short_term_investments = cash_and_cash_equivalents + short_term_investments
            bs_stmt['cash_and_short_term_investments'] = cash_and_short_term_investments
    # PPE
    if operating_lease:
        bs_stmt['property_plant_equipment_net'] = property_plant_equipment_net + operating_lease
    # payables_and_expenses
    payables_and_expenses = bs_stmt.get('payables_and_expenses')
    if not payables_and_expenses:
        account_payables = bs_stmt.get('account_payables') or 0
        accrued_liabilities_current = bs_stmt.get('accrued_liabilities_current') or 0
        if account_payables or accrued_liabilities_current:
            bs_stmt['payables_and_expenses'] = account_payables + accrued_liabilities_current
    # Net Debt
    if debt and cash_and_short_term_investments and (net_debt  := debt - cash_and_short_term_investments) > 0:
        bs_stmt['net_debt'] = net_debt
    # Calc other positions
    current_assets_total = sum(bs_stmt.get(position) or 0 for position in utils.current_assets_list)
    non_current_assets_total = sum(bs_stmt.get(position) or 0 for position in utils.non_current_assets_list)
    current_liabilities_total = sum(bs_stmt.get(position) or 0 for position in utils.current_liabilities_list)
    non_current_liabilities_total = sum(bs_stmt.get(position) or 0 for position in utils.non_current_liabilities_list)
    if current_assets and current_assets_total:
        bs_stmt['other_current_assets'] = current_assets - current_assets_total
    if non_current_assets and non_current_assets_total:
        bs_stmt['other_non_current_assets'] = non_current_assets - non_current_assets_total
    if current_liabilities and current_liabilities_total:
        bs_stmt['other_current_liabilities'] = current_liabilities - current_liabilities_total
    if non_current_liabilities and non_current_liabilities_total:
        bs_stmt['other_non_current_liabilities'] = non_current_liabilities - non_current_liabilities_total

    # Converts any numpy data types to Python native data types and None to zero
    return {k: v.item() if isinstance(v, np.generic) else v for k, v in bs_stmt.items()}


def validate_income_statement(inc_stmt: dict[str,float], cf_stmt: dict[str, float]) -> dict[str,float] | None:
    # Revenue
    # – COGS
    # = Gross Profit
    # – Operating Expenses
    # = Operating Income
    # + Other Income
    # – Other Expenses
    # = Earnings Before Tax(EBT)
    # – Income Tax
    # = Net# Income
    revenue = inc_stmt['revenue']
    cost_of_revenue = abs(inc_stmt['cost_of_revenue']) if inc_stmt['cost_of_revenue'] else None
    gross_profit = inc_stmt['gross_profit']
    operating_income = inc_stmt['operating_income']
    operating_expenses = abs(inc_stmt['operating_expenses']) if inc_stmt['operating_expenses'] else None
    selling_general_and_administrative_expense = abs(inc_stmt['selling_general_and_administrative_expense']) if inc_stmt['selling_general_and_administrative_expense'] else None
    research_and_development_expenses = abs(inc_stmt['research_and_development_expenses']) if inc_stmt['research_and_development_expenses'] else None
    net_non_operating_income = inc_stmt['net_non_operating_income']
    ebt = inc_stmt['ebt']
    interest_income = inc_stmt['interest_income']
    interest_expense = abs(inc_stmt['interest_expense'] or 0)
    net_interest = inc_stmt['net_interest']
    da = abs(cf_stmt['operating_da'] or 0) if cf_stmt else None
    net_income_including_non_controlling_interests = inc_stmt['net_income_including_non_controlling_interests']
    net_income_non_controlling_interests = inc_stmt['net_income_non_controlling_interests']
    net_income = inc_stmt.get('net_income')
    other_operating_expenses, expenses, ebit, ebitda = None, None, None, None
    if revenue:
        if gross_profit:
            cost_of_revenue = revenue - gross_profit
        elif cost_of_revenue:
            gross_profit = revenue - cost_of_revenue
    if operating_income:
        if gross_profit and operating_income:
            operating_expenses = gross_profit - operating_income
        if ebt: # todo should I add here not net_non_operating_income?
            net_non_operating_income = ebt - operating_income
        elif net_non_operating_income is not None:
            ebt = operating_income + net_non_operating_income
        if not revenue and not operating_expenses and operating_income < 0:
            operating_expenses = abs(operating_income)
    elif operating_expenses and gross_profit:
        operating_income = gross_profit - operating_expenses
    if operating_expenses:
        # Redeclaring the same variables to prevent None to be 0 in database
        sga, rd = selling_general_and_administrative_expense or 0, research_and_development_expenses or 0
        other_operating_expenses = operating_expenses - abs(sga) - abs(rd)
        # todo find a way to display correct values if other_operating_expenses are negative
    interest_flag = True
    if not interest_expense:
        if interest_income and net_interest:
            interest_expense = abs(interest_income - net_interest)
        elif net_interest:
            interest_expense = abs(net_interest) if net_interest < 0 else 0 # Only expense counts in EBIT calculation
            interest_flag = False
    if all([not interest_income, interest_expense, net_interest, interest_flag]):
        interest_income = net_interest + interest_expense
    if all([not net_interest, interest_income, interest_expense, interest_flag]):
        net_interest = interest_income = interest_expense

    if ebt and interest_expense is not None:
        ebit = ebt + interest_expense
    if ebit and da:
        ebitda = ebit + da
    if net_income:
        if net_income_non_controlling_interests:
            net_income_including_non_controlling_interests = net_income - net_income_non_controlling_interests
        elif net_income_including_non_controlling_interests:
            net_income_non_controlling_interests = net_income - net_income_including_non_controlling_interests
    if revenue and net_income:
        expenses = revenue - net_income
    elif operating_expenses:
        expenses = operating_expenses + interest_expense if interest_expense > 0 else operating_expenses # interest_expense can be positive due to variable interest_inc_exp,so that's why > 0


    inc_stmt['cost_of_revenue'] = cost_of_revenue
    inc_stmt['gross_profit'] = gross_profit
    inc_stmt['operating_income'] = operating_income
    inc_stmt['operating_expenses'] = operating_expenses
    inc_stmt['other_operating_expenses'] = other_operating_expenses
    inc_stmt['selling_general_and_administrative_expense'] = selling_general_and_administrative_expense
    inc_stmt['research_and_development_expenses'] = research_and_development_expenses
    inc_stmt['net_non_operating_income'] = net_non_operating_income
    inc_stmt['expenses'] = expenses
    inc_stmt['ebt'] = ebt
    inc_stmt['income_tax'] = abs(inc_stmt.get('income_tax') or 0)
    inc_stmt['interest_income'] = interest_income
    inc_stmt['interest_expense'] = interest_expense
    inc_stmt['net_interest'] = net_interest
    inc_stmt['deprecation_and_amortization'] = da
    inc_stmt['ebit'] = ebit
    inc_stmt['ebitda'] = ebitda
    inc_stmt['net_income_including_non_controlling_interests'] = net_income_including_non_controlling_interests
    inc_stmt['net_income_non_controlling_interests'] = net_income_non_controlling_interests

    # Remove keys
    inc_stmt.pop('operating_expenses_all')
    # Converts any numpy data types to Python native data types and None to zero
    return {k: v.item() if isinstance(v, np.generic) else v for k, v in inc_stmt.items()}

def validate_cashflow_statement(df_instant_start: pd.DataFrame, df_instant_end: pd.DataFrame, df_period: pd.DataFrame, cf_stmt: dict[str,float], acc_standard: str) -> dict[str,float] | None:
    # CAPEX calculation
    capital_expenditure = None
    if not df_period.empty and {'concept', 'numeric_value'}.issubset(df_period.columns):
        df_ppe = df_period[df_period['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['net_purchase_sale_ppe'])]
        capex_values = pd.to_numeric(df_ppe['numeric_value'], errors='coerce')
        capex_sum = capex_values[capex_values < 0].sum()
        capital_expenditure = capex_sum * config.scale_factor if capex_sum else 0
        cf_stmt['capital_expenditure'] = capital_expenditure
    else:
        logger.warning('df_period empty or missing concept/numeric_value columns')

    # # Debt issuance and repayment calculation
    # df_debt = df_period[df_period['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['net_debt_issuance'])]
    # debt_values = pd.to_numeric(df_debt['numeric_value'], errors='coerce')
    # # Debt Issued
    # debt_issued = debt_values[debt_values > 0].sum()
    # if debt_issued:
    #     cf_stmt['debt_issued'] = debt_issued
    # # Debt Repayment
    # debt_repayment = debt_values[debt_values < 0].sum()
    # if debt_repayment:
    #     cf_stmt['debt_repayment'] = debt_repayment

    # Free Cash Flow
    operating_cash_flow = cf_stmt.get('operating_cash_flow')
    if operating_cash_flow and capital_expenditure:
        free_cash_flow = operating_cash_flow + capital_expenditure
        cf_stmt['free_cash_flow'] = free_cash_flow

    # Cash end
    required_columns = {'statement_type', 'concept', 'numeric_value'}
    if not df_instant_start.empty and not df_instant_end.empty and required_columns.issubset(df_instant_start) and required_columns.issubset(df_instant_end):
        df_cash_end = df_instant_end[df_instant_end['statement_type'].isin(['CashFlowStatement','BalanceSheet'])]
        df_cash_end = df_cash_end[df_cash_end['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['end_cash_balance'])]
        end_cash_balance = df_cash_end['numeric_value'].max()
        end_cash_balance = end_cash_balance * config.scale_factor if not pd.isna(end_cash_balance) else None
        if end_cash_balance:
            cf_stmt['end_cash_balance'] = end_cash_balance
        # Cash start
        net_change = cf_stmt.get('change_in_cash')
        if net_change and end_cash_balance:
            start_cash_balance = end_cash_balance - net_change
        else:
            df_cash_start = df_instant_start[df_instant_start['statement_type'].isin(['CashFlowStatement', 'BalanceSheet'])]
            df_cash_start = df_cash_start[df_cash_start['concept'].isin(XBRLTagMapper.xbrl_tags[acc_standard]['CashFlowStatement']['start_cash_balance'])]
            start_cash_balance = df_cash_start['numeric_value'].max()
            start_cash_balance = start_cash_balance * config.scale_factor if not pd.isna(start_cash_balance) else None
        if start_cash_balance:
            cf_stmt['start_cash_balance'] = start_cash_balance
        # Cash change
        if start_cash_balance and end_cash_balance and not net_change:
            cf_stmt['change_in_cash'] = end_cash_balance - start_cash_balance
    else:
        logger.warning('df_instant_start or df_instant_end empty or missing concept/numeric_value columns')

    # calculate other activities
    if operating_cash_flow:
        cf_stmt['other_operating_activities'] = (operating_cash_flow - (cf_stmt['operating_net_income'] or 0)
                                                - (cf_stmt['operating_da'] or 0)- (cf_stmt['deferred_income_tax'] or 0)
                                                - (cf_stmt['share_based_compensation'] or 0) - (cf_stmt['change_working_capital'] or 0))
    if investing_cash_flow := cf_stmt['investing_cash_flow']:
        cf_stmt['other_investing_activities'] = (investing_cash_flow - (cf_stmt['capital_expenditure'] or 0)
                                                 - (cf_stmt['net_purchase_sale_ppe'] or 0) - (cf_stmt['net_purchase_sale_investments'] or 0)
                                                 - (cf_stmt['net_business_acquisitions'] or 0) - (cf_stmt['net_loan_lease_activity'] or 0))
    if financing_cash_flow := cf_stmt['financing_cash_flow']:
        cf_stmt['other_financing_activities'] = (financing_cash_flow - (cf_stmt['dividends_paid'] or 0)
                                                 - (cf_stmt['net_equity_issuance'] or 0) - (cf_stmt['net_debt_issuance'] or 0))

    # Converts any numpy data types to Python native data types and None to zero
    return {k: v.item() if isinstance(v, np.generic) else v for k, v in cf_stmt.items()}


def get_calendar_period(date_str: str, form: str) -> str | None:
    """
    Returns the calendar period (YYYY for annual forms, YYYYQ1-YYYYQ4 for quarterly)
    based on a date string and SEC form type.
    """
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        if form.upper() in utils.forms['annual']:
            return str(date.year)
        # Quarter calculation
        quarter = (date.month - 2) // 3 + 1  # Added date.month - 2 instead of date.month - 1 because I want to have a buffer period of one month.
        if quarter == 0:                     # This way '2024-06-01' will be counted as Q2 instead of Q3/
            return f"{date.year - 1}Q{4}"
        else:
            return f"{date.year}Q{quarter}"
    except (ValueError, TypeError):
        pass
    return None



def get_fiscal_period(entity_info:dict, form:str) -> str | None:
    """
    Returns a formatted fiscal period string based on the SEC form type.
        - For annual forms (10-K, 20-F, etc.): Returns the fiscal year (YYYY).
        - For quarterly forms: Returns fiscal year + quarter (YYYYQ1, YYYYQ2, etc.).
        - Returns None if required data is missing or invalid.
    """
    try:
        if form in utils.forms['annual']:
            return entity_info['fiscal_year']
        else:
            return entity_info['fiscal_year'] + entity_info['fiscal_period'].upper()
    except (KeyError,AttributeError):
        pass
    return None


def get_statement_by_xbrl_query(filing: edgar.Filing, stmt_name: str) -> pd.DataFrame:
    if stmt_name == 'BalanceSheet':
        df = filing.xbrl().query().by_statement_type(stmt_name).by_instant_date(filing.period_of_report).to_dataframe('concept', 'label', 'period_instant', 'numeric_value')
        if not df.empty and {'concept', 'label', 'period_instant', 'numeric_value'}.issubset(df.columns):
            df.drop(columns=['period_instant'],inplace=True)
            df.rename(columns={'numeric_value':filing.period_of_report},inplace=True)
            df['concept'] = df['concept'].str.replace(':','_')
            return df

    elif stmt_name == 'IncomeStatement' or stmt_name == 'CashFlowStatement':
        df = filing.xbrl().query().by_statement_type(stmt_name).to_dataframe('concept', 'label', 'period_start','period_end', 'numeric_value')
        if df.empty or not {'concept', 'label', 'period_start','period_end', 'numeric_value'}.issubset(df.columns): return pd.DataFrame(data=None)
        df = df[df['period_end'] == str(filing.period_of_report)]
        df = df.drop_duplicates(subset=['concept','numeric_value'])
        # Calculate day differences
        days_diff = 90 if filing.form in utils.forms['quarterly'] else 365

        # Convert to datetime and calculate day differences
        df = df.copy()  # Explicit copy to avoid the warning
        df.loc[:, 'start'] = pd.to_datetime(df['period_start'])
        df.loc[:, 'end'] = pd.to_datetime(df['period_end'])
        df.loc[:, 'days'] = (df['end'] - df['start']).dt.days.fillna(0)

        # Find the period closest to target days_diff
        df['abs_diff'] = (df['days'] - days_diff).abs()
        # First get the counts of each abs_diff value
        value_counts = df['abs_diff'].value_counts()
        # Filter for counts > 10 and get the minimum abs_diff
        min_diff = value_counts[value_counts > 10].index.min()
        # Check if min_diff is float nan to prevent convert float NaN to integer
        if pd.isna(min_diff): return pd.DataFrame(data=None)
        df = df[df['abs_diff'] == int(min_diff)]
        df.drop(columns=['abs_diff', 'start','end', 'days', 'period_start','period_end'],inplace=True)
        df.rename(columns={'numeric_value': filing.period_of_report},inplace=True)
        df['concept'] = df['concept'].str.replace(':', '_')
        return df

    return pd.DataFrame(data=None)


def get_q4_statements(a_statements:dict,share_id:int, report_period: date) -> dict:
    # Q1 report date must be within 330 days
    start_date = (report_period - timedelta(days=330))
    # Balance Sheet already displays correct Q4 data
    a_bs = a_statements.pop('BalanceSheet')
    q_statements ={'BalanceSheet': a_bs}
    # Other data

    filing_date = a_bs['filing_date']
    report_date = a_bs['date']
    currency_id = a_bs['currency_id']
    fiscal_period_id  = a_bs['fiscal_period_id'] + 4
    calendar_period_id = utils.report_periods.get(get_calendar_period(report_date,'10-Q'))
    q_statements['BalanceSheet']['fiscal_period_id'] = fiscal_period_id
    q_statements['BalanceSheet']['calendar_period_id'] = calendar_period_id
    q_statements['BalanceSheet']['date'] = report_date
    q_statements['BalanceSheet']['filing_date'] = filing_date
    q_statements['BalanceSheet']['currency_id'] = currency_id
    q_statements['BalanceSheet']['report_type'] = 'q'

    # Income and Cashflow Statement calculation
    for key, value in a_statements.items():
        # Sum Q1, Q2 and Q3 data
        non_sum_columns = {'operating_expenses_all', 'eps', 'diluted_eps', 'start_cash_balance','end_cash_balance', 'filing_date', 'share_id',
                           'calendar_period_id', 'date', 'report_type', 'currency_id', 'fiscal_period_id'}
        sum_columns = set(value.keys()).difference(non_sum_columns)
        sum_q1_q2_q3 = db_ops.query_3_quarter_sums(share_id, sum_columns, utils.camel_to_snake(key), start_date, report_period)
        if sum_q1_q2_q3:
            a_stmt = a_statements[key]
            q4_stmt = dict()
            for k in sum_q1_q2_q3: # Calculate difference between A and sum of 3 quarters
                a, q_sum = a_stmt[k], sum_q1_q2_q3[k]
                if a is not None and q_sum is not None:
                    q4_stmt[k] = a - q_sum
                else:
                    q4_stmt[k] = a
            if key == 'IncomeStatement':
                # Average shares basic
                avg_shares_basic_q_sum = sum_q1_q2_q3['avg_shares_basic']
                avg_shares_basic_a = a_stmt['avg_shares_basic']
                # Average diluted shares
                avg_shares_diluted_q_sum = sum_q1_q2_q3['avg_shares_diluted']
                avg_shares_diluted_a = a_stmt['avg_shares_diluted']

                # Average shares calculation
                # Basic
                if avg_shares_basic_q_sum and avg_shares_basic_a:
                    q4_stmt['avg_shares_basic'] = 4 * avg_shares_basic_a - avg_shares_basic_q_sum
                else:
                    q4_stmt['avg_shares_basic'] = avg_shares_basic_a  if avg_shares_basic_a else avg_shares_basic_q_sum/3 if avg_shares_basic_q_sum else 0
                # Diluted
                if avg_shares_diluted_q_sum and avg_shares_diluted_a:
                    q4_stmt['avg_shares_diluted'] = 4 * avg_shares_diluted_a - avg_shares_diluted_q_sum
                else:
                    q4_stmt['avg_shares_diluted'] = avg_shares_diluted_a  if avg_shares_diluted_a else avg_shares_diluted_q_sum/3 if avg_shares_diluted_q_sum else 0
                # EPS calculation
                # EPS basic
                if (net_income := q4_stmt['net_income']) and (avg_shares_basic := q4_stmt['avg_shares_basic']):
                    q4_stmt['eps'] = net_income / avg_shares_basic
                else:
                    q4_stmt['eps'] = 0
                # EPS diluted - No data for net income adjusted for preferred shares
                q4_stmt['diluted_eps'] = 0
            elif key == 'CashFlowStatement':
                if q4_stmt['change_in_cash'] and a_stmt['end_cash_balance']:
                    q4_stmt['end_cash_balance'] = a_stmt['end_cash_balance']
                    q4_stmt['start_cash_balance'] = q4_stmt['end_cash_balance'] - q4_stmt['change_in_cash']
                else:
                    q4_stmt['start_cash_balance'] = 0


            # Other data
            q4_stmt['calendar_period_id'] = calendar_period_id
            q4_stmt['fiscal_period_id'] = fiscal_period_id
            q4_stmt['report_type'] = 'q'
            q4_stmt['date'] = report_date
            q4_stmt['filing_date'] = filing_date
            q4_stmt['currency_id'] = currency_id
            q4_stmt['share_id'] = share_id

            # Add statement to returning dict
            q_statements[key] = q4_stmt

    return q_statements


def get_quarterly_cash_flow_statement(cur_cf:dict, share_id, period_start: str, period_end: str) -> Optional[dict[str,Union[int,float,None]]]:
    prev_cf = db_ops.query_previous_cash_flow_statement(share_id,period_start,period_end)
    if not prev_cf: return None
    other_data = {'date', 'report_type', 'share_id', 'fiscal_period_id', 'calendar_period_id', 'currency_id', 'filing_date'}
    excluded_columns = {'start_cash_balance', 'end_cash_balance'}
    calc_columns = set(prev_cf.keys()).difference(excluded_columns)
    # Calculate difference
    calc_cf = dict()
    for k in calc_columns:
        if k in other_data:
            calc_cf[k] = cur_cf[k]
            continue
        prev_val, cur_val = prev_cf[k], cur_cf[k]
        if prev_val is not None and cur_val is not None:
            calc_cf[k] = float(cur_val) - float(prev_val)
        elif cur_val is not None:
            calc_cf[k] = float(cur_val)
        else:
            calc_cf[k] = 0

    return calc_cf if calc_cf else None