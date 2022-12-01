-- insert into ETL_AF.DEV_DB.fact_Stock_History_Group4

-- select * from (
-- select * from US_STOCKS_DAILY.PUBLIC.Stock_History
-- EXCEPT
-- select * from ETL_AF.DEV_DB.fact_Stock_History_Group4
-- )

merge into ETL_AF.DEV_DB.fact_Stock_History_Group4 as a using US_STOCKS_DAILY.PUBLIC.Stock_History as b
    on (a.symbol = b.symbol and a.date = b.date and a.open = b.open and a.high = b.high and a.low = b.low and a.close = b.close 
    and a.volume = b.volume and a.adjclose = b.adjclose)
    -- when matched then 
    --    update set a.symbol = b.symbol, a.date = b.date, a.open = b.open, a.high = b.high, a.low = b.low, a.close = b.close,
    -- a.volume = b.volume, a.adjclose = b.adjclose
    when not matched then 
        insert (symbol, date, open, high, low, close, volume, adjclose) values (b.symbol, b.date, b.open, b.high, b.low, b.close, b.volume, b.adjclose);


