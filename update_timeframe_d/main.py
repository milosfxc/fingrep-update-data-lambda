from db_ops import get_existing_tickers
from fingrep_service import get_grouped_daily_bars, insert_grouped_daily_bars
from utils import get_utc_date
if __name__ == '__main__':
    existing_tickers = get_existing_tickers()
    df_grouped_daily = get_grouped_daily_bars(date_str=get_utc_date(days=0))
    if not df_grouped_daily.empty:
        # Data frame for existing tickers
        df_grouped_daily['id'] = df_grouped_daily['T'].map(existing_tickers)
        df_grouped_daily_existing = df_grouped_daily.dropna(subset=['id'])
        # Importing data for existing tickers
        insert_grouped_daily_bars(df_grouped_daily_existing.copy())
