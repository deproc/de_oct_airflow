insert into ETL_AF.DEV_DB.fact_Stock_History_Group4

select * from 
US_STOCKS_DAILY.PUBLIC.Stock_History
EXCEPT
ETL_AF.DEV_DB.fact_Stock_History_Group4
    



