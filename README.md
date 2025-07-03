# fingrep-update-data-lambda
* Check for new IPOs
* Check for ticker changes 
* Check for splits

# initial setup
* Get historical prices
* Get company info data
* Get historical economic calendar
* Get historical split data

https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apiKey=KAKTnsmvIxPYvpwuzancIju96yzwiU5U
https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apikey=KAKTnsmvIxPYvpwuzancIju96yzwiU5U


# finviz2yahoo mapping for sectors and industries
* Financial -> Financial Services (sector)
* None -> Exchange Traded Fund (industry)
* Closed-End Fund - Equity -> Asset Management (industry)
* Closed-End Fund - Foreign -> Asset Management (industry)
* Closed-End Fund - Debt -> Asset Management (industry)

# Dolt pull from remote
* sudo dolt pull origin master

# Fundamentals
* get 10-K, 10-Q, 20-F, 6-F filings by a company
* extract basic dates //Can I extract dates in 6-F form?
* extract currency // AI must extract currency from table in 6-F
* Get concepts from dataframe
* Check if XBRLRegistry already has these concepts
    * Use existing to extract all available data
    * Use DeepSeek to get xbrl mappings
* return dataframe for insertion