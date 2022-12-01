MERGE INTO "ETL_AF"."DEV_DB"."DIM_COMPANYPROFILE_GROUP3_TEST" t
USING "US_STOCKS_DAILY"."PUBLIC"."COMPANY_PROFILE" s
ON t.id = MD5_NUMBER_LOWER64(s.symbol)
WHEN matched
    AND NOT(
    t.price = s.price
    AND t.beta = s.beta
    AND t.volavg = s.volavg
    AND t.mktcap = s.mktcap
    AND t.lastdiv = s.lastdiv
    AND t.range = t.range
    AND t.changes = s.changes
    AND t.companyname = s.companyname
    AND t.exchange = s.exchange
    AND t.industry = s.industry
    AND t.website = s.website
    AND t.description = s.description
    AND t.ceo = s.ceo
    AND t.sector = s.sector
    AND t.dcfdiff = s.dcfdiff
    AND t.dcf = s.dcf
    )
THEN
    UPDATE SET t.price = s.price
        , t.beta = s.beta
        , t.volavg = s.volavg
        , t.mktcap = s.mktcap
        , t.lastdiv = s.lastdiv
        , t.range = t.range
        , t.changes = s.changes
        , t.companyname = s.companyname
        , t.exchange = s.exchange
        , t.industry = s.industry
        , t.website = s.website
        , t.description = s.description
        , t.ceo = s.ceo
        , t.sector = s.sector
        , t.dcfdiff = s.dcfdiff
        , t.dcf = s.dcf
WHEN NOT matched THEN
    INSERT (
      id
    , symbol
    , price
    , beta
    , volavg
    , mktcap
    , lastdiv
    , range
    , changes
    , companyname
    , exchange
    , industry
    , website
    , description
    , ceo
    , sector
    , dcfdiff
    , dcf)
    values (
      MD5_NUMBER_LOWER64(s.symbol)
    , s.symbol
    , s.price
    , s.beta
    , s.volavg
    , s.mktcap
    , s.lastdiv
    , s.range
    , s.changes
    , s.companyname
    , s.exchange
    , s.industry
    , s.website
    , s.description
    , s.ceo
    , s.sector
    , s.dcfdiff
    , s.dcf
    );