use schema ETL_AF.DEV_DB;

CREATE OR REPLACE TEMPORARY TABLE ETL_AF.DEB_DB.PRESTG_PRODUCT_TEMP_GROUP4 
(
    INVOICENO NUMBER(38,0)
    , STOCKCODE VARCHAR(36)
    , DESCRIPTION VARCHAR(100)
    , QUANTITY NUMBER(38,0)
    , INVOICEDATE TIMESTAMP NTZ(9)
    , MONTH NUMBER(2,0)
    , DAY NUMBER(2,0)
    , UNITPRICE NUMBER(10,0)
    , CUSTOMERID NUMBER(38,0)
    ,COUNTRY VARCHAR(100)
    , COUNTRYCODE VARCHAR(2)
);


