select
           code,
           name,
           date,
           scale,
           profit,
           success_cnt/fund_cnt success_rate
from (
    select
           code,
           name,
           date,
           scale,
           profit,
           count(1) over(partition by code order by date) fund_cnt,
           count(if(profit>0,code,null)) over(partition by code order by date) success_cnt
    from (
        select
            code,
            name,
            date,
            scale,
            if(ahead_rate_1>0, ahead_rate_1, ahead_rate_2) profit
        from (
            select
                code,
                name,
                date,
                scale,
                (close-close_1)/close_1 rate_1,
                (close_1-close_2)/close_2 rate_2,
                (close_2-close_3)/close_3 rate_3,
                (close_3-close_4)/close_4 rate_4,
                (close_4-close_5)/close_5 rate_5,
                (close_5-close_6)/close_6 rate_6,
                (ahead_1-close)/close ahead_rate_1,
                (ahead_2-close)/close ahead_rate_2
            from (
                select t1.code,
                       t3.name,
                       date,
                       close,
                       t2.scale,
                       lag(close, 1) over (partition by t1.code order by date) close_1,
                       lag(close, 2) over (partition by t1.code order by date) close_2,
                       lag(close, 3) over (partition by t1.code order by date) close_3,
                       lag(close, 4) over (partition by t1.code order by date) close_4,
                       lag(close, 5) over (partition by t1.code order by date) close_5,
                       lag(close, 6) over (partition by t1.code order by date) close_6,
                       lead(close, 1) over (partition by t1.code order by date) ahead_1,
                       lead(close, 2) over (partition by t1.code order by date) ahead_2
                from df t1
                join scale_df t2
                  on t1.code=t2.code
                left join fund_etf_fund_daily_em_df t3
                       on t1.code=t3.code
                where t2.scale>=10
                 ) t
             )t
        where rate_1<0 and rate_2<0
              and rate_3<3 and rate_4<0
              and rate_5<0 and rate_6>0
         ) t
     ) t
where success_cnt/fund_cnt>=0.6
order by date, code
;

