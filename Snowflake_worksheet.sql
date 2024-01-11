CREATE DATABASE FINANCE;
CREATE SCHEMA MORTGAGE;

create or replace function FINANCE.MORTGAGE.Forecast_Stock_value(ticker string, val float, dt date, period number)
returns table(ticker string, y float, d string)
language python
runtime_version = 3.8
packages = ('prophet')
handler = 'StockValue'
as $$
import pandas as pd
from prophet import Prophet
class StockValue:
    def __init__(self):
        self._dates = []
        self._values = []
        
    def process(self, ticker, v, d, period):
        self._dates.append(d)
        self._values.append(v)
        self._ticker = ticker
        self._period = period
        return ((ticker, v, d), )
    def end_partition(self):
    
        df = pd.DataFrame(list(zip(self._dates, self._values)),
               columns =['ds', 'y'])
               
        model = Prophet()
        model.fit(df)
        future_df = model.make_future_dataframe(
            periods=self._period, 
            include_history=False)
        forecast = model.predict(future_df)
        for row in forecast.itertuples():
            yield(self._ticker, row.yhat, row.ds)
$$;


with data as (
select "Company Ticker","Units","Date","Value"
from ECONOMY_DATA_ATLAS.ECONOMY."USINDSSP2020" 
where 
"Indicator Name"='Close' and "Indicator Unit"='USD' and
"Stock Exchange Name"='NASDAQ' and "Company Ticker" in ('AMZN','NFLX','AAPL','META')
and "Date">'2021-01-01'
)
select p.*
from data
    , table(FINANCE.MORTGAGE.Forecast_Stock_value("Company Ticker", "Value", "Date", 200)  -- 200 days away from the last data value 
      over (partition by "Company Ticker")) p;
	  