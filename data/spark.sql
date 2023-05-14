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

select
           code,
           name,
           date,
           scale,
           profit,
           success_cnt/fund_cnt success_rate,
           rate_1,
           rate_2,
           rate_3,
            rate_4,
            rate_5,
            rate_6,
            code_cnt,
            rate_1+rate_2+rate_3+rate_4+rate_5 rate_sum
from (
    select
           code,
           name,
           date,
           scale,
           profit,
           count(1) over(partition by code order by date) fund_cnt,
           count(if(profit>0,code,null)) over(partition by code order by date) success_cnt,
            rate_1,
            rate_2,
            rate_3,
            rate_4,
            rate_5,
            rate_6,
            count(code) over(partition by date) code_cnt
    from (
        select
            code,
            name,
            date,
            scale,
            if(ahead_rate_1>0, ahead_rate_1, ahead_rate_2) profit,
            rate_1,
            rate_2,
            rate_3,
            rate_4,
            rate_5,
            rate_6
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

select code,
       name,
       date,
       scale,
       profit,
       pattern,
       success_cnt / fund_cnt success_rate,
       success_cnt,
       fund_cnt
from (select code,
             name,
             date,
             scale,
             profit,
             pattern,
             count(1) over (partition by pattern, code order by date)                       fund_cnt,
             count(if(profit > 0, 1, null)) over (partition by pattern, code order by date) success_cnt
      from (select code,
                   name,
                   date,
                   scale,
                   if(ahead_rate_1 > 0, ahead_rate_1, ahead_rate_2) profit,
                   abs(nvl(t2.pattern1, rate1) - rate1) +
                   abs(nvl(t2.pattern2, rate2) - rate2) +
                   abs(nvl(t2.pattern3, rate3) - rate3) +
                   abs(nvl(t2.pattern4, rate4) - rate4) +
                   abs(nvl(t2.pattern5, rate5) - rate5) +
                   abs(nvl(t2.pattern6, rate6) - rate6) +
                   abs(nvl(t2.pattern7, rate7) - rate7) +
                   abs(nvl(t2.pattern8, rate8) - rate8) +
                   abs(nvl(t2.pattern9, rate9) - rate9) +
                   abs(nvl(t2.pattern10, rate10) - rate10)          flag,
                   concat_ws("", nvl(pattern1, "*"), nvl(pattern2, "*"), nvl(pattern3, "*"), nvl(pattern4, "*"),
                             nvl(pattern5, "*"),
                             nvl(pattern6, "*"), nvl(pattern7, "*"), nvl(pattern8, "*"), nvl(pattern9, "*"),
                             nvl(pattern10, "*"))                   pattern
            from (select code,
                         name,
                         date,
                         scale,
                         close,
                         if((close - close_1) > 0, 1, 0)    rate1,
                         if((close_1 - close_2) > 0, 1, 0)  rate2,
                         if((close_2 - close_3) > 0, 1, 0)  rate3,
                         if((close_3 - close_4) > 0, 1, 0)  rate4,
                         if((close_4 - close_5) > 0, 1, 0)  rate5,
                         if((close_5 - close_6) > 0, 1, 0)  rate6,
                         if((close_6 - close_7) > 0, 1, 0)  rate7,
                         if((close_7 - close_8) > 0, 1, 0)  rate8,
                         if((close_8 - close_9) > 0, 1, 0)  rate9,
                         if((close_9 - close_10) > 0, 1, 0) rate10,
                         (ahead_1 - close) / close          ahead_rate_1,
                         (ahead_2 - close) / close          ahead_rate_2
                  from (select t1.code,
                               t3.name,
                               date,
                               close,
                               t2.scale,
                               lag(close, 1) over (partition by t1.code order by date)  close_1,
                               lag(close, 2) over (partition by t1.code order by date)  close_2,
                               lag(close, 3) over (partition by t1.code order by date)  close_3,
                               lag(close, 4) over (partition by t1.code order by date)  close_4,
                               lag(close, 5) over (partition by t1.code order by date)  close_5,
                               lag(close, 6) over (partition by t1.code order by date)  close_6,
                               lag(close, 7) over (partition by t1.code order by date)  close_7,
                               lag(close, 8) over (partition by t1.code order by date)  close_8,
                               lag(close, 9) over (partition by t1.code order by date)  close_9,
                               lag(close, 10) over (partition by t1.code order by date) close_10,
                               lead(close, 1) over (partition by t1.code order by date) ahead_1,
                               lead(close, 2) over (partition by t1.code order by date) ahead_2
                        from df t1
                                 join scale_df t2
                                      on t1.code = t2.code
                                 left join fund_etf_fund_daily_em_df t3
                                           on t1.code = t3.code) t) t1
                     join pattern t2
                          on 1 = 1) t
      where flag = 0) t
where success_cnt / fund_cnt>=0.8 and fund_cnt>=5 and scale>=10
order by date desc, scale desc, pattern
;