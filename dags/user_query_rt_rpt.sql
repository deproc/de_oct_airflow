use schema beaconfire.prod_db;

create or replace local temp table query_hist as 
select *
    , row_number() over(partition by user_name order by start_time asc) rn
from prestg_account_query_hist;


create or replace table user_query_rt_rpt as
select 
    query_dt
    , query_hr
    , user_name
    , round(sum(activity_ms)/60000) as activity_min
    , case when activity_min < 3 then 0 else 1 end as activity_flag
    , sysdate() as load_utc_ts
from 
(
    select s.start_time
        , e.start_time as next_start_time
        , s.user_name
        , date(s.start_time) as query_dt
        , hour(s.start_time) as query_hr
        , timediff(ms, s.start_time, e.start_time) as activity_ms
    from query_hist s
    left outer join query_hist e
    on s.rn+1 = e.rn
    and s.user_name = e.user_name
    where activity_ms <= 60000*4
)q
group by 1,2,3
having activity_min <> 0
;