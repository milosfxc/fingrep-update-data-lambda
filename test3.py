import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 400)


# Sample DataFrame
df = pd.read_csv('data/finviz_sic.csv')
print(df)
print(len(df['industry'].unique()))