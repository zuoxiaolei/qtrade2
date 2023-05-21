-- qtrade
select code,
       name,
       date,
       scale,
       profit,
       success_cnt / fund_cnt success_rate
from (select code,
             name,
             date,
             scale,
             profit,
             count(1) over (partition by code order by date)                          fund_cnt,
             count(if(profit > 0, code, null)) over (partition by code order by date) success_cnt
      from (select code,
                   name,
                   date,
                   scale,
                   if(ahead_rate_1 > 0, ahead_rate_1, ahead_rate_2) profit
            from (select code,
                         name,
                         date,
                         scale,
                         (close - close_1) / close_1   rate_1,
                         (close_1 - close_2) / close_2 rate_2,
                         (close_2 - close_3) / close_3 rate_3,
                         (close_3 - close_4) / close_4 rate_4,
                         (close_4 - close_5) / close_5 rate_5,
                         (close_5 - close_6) / close_6 rate_6,
                         (ahead_1 - close) / close     ahead_rate_1,
                         (ahead_2 - close) / close     ahead_rate_2
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
                               lead(close, 1) over (partition by t1.code order by date) ahead_1,
                               lead(close, 2) over (partition by t1.code order by date) ahead_2
                        from df t1
                                 join scale_df t2
                                      on t1.code = t2.code
                                 left join fund_etf_fund_daily_em_df t3
                                           on t1.code = t3.code) t) t
            where rate_1 < 0
              and rate_2 < 0
              and rate_3 < 3
              and rate_4 < 0
              and rate_5 < 0
              and rate_6 > 0) t) t
where success_cnt / fund_cnt >= 0.6
order by date, code
;

-- qtrade2
select code,
       name,
       date,
       scale,
       profit,
       success_cnt / fund_cnt                     success_rate,
       rate_1,
       rate_2,
       rate_3,
       rate_4,
       rate_5,
       rate_6,
       code_cnt,
       rate_1 + rate_2 + rate_3 + rate_4 + rate_5 rate_sum
from (select code,
             name,
             date,
             scale,
             profit,
             count(1) over (partition by code order by date)                          fund_cnt,
             count(if(profit > 0, code, null)) over (partition by code order by date) success_cnt,
             rate_1,
             rate_2,
             rate_3,
             rate_4,
             rate_5,
             rate_6,
             count(code) over (partition by date)                                     code_cnt
      from (select code,
                   name,
                   date,
                   scale,
                   if(ahead_rate_1 > 0, ahead_rate_1, ahead_rate_2) profit,
                   rate_1,
                   rate_2,
                   rate_3,
                   rate_4,
                   rate_5,
                   rate_6
            from (select code,
                         name,
                         date,
                         scale,
                         (close - close_1) / close_1   rate_1,
                         (close_1 - close_2) / close_2 rate_2,
                         (close_2 - close_3) / close_3 rate_3,
                         (close_3 - close_4) / close_4 rate_4,
                         (close_4 - close_5) / close_5 rate_5,
                         (close_5 - close_6) / close_6 rate_6,
                         (ahead_1 - close) / close     ahead_rate_1,
                         (ahead_2 - close) / close     ahead_rate_2
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
                               lead(close, 1) over (partition by t1.code order by date) ahead_1,
                               lead(close, 2) over (partition by t1.code order by date) ahead_2
                        from df t1
                                 join scale_df t2
                                      on t1.code = t2.code
                                 left join fund_etf_fund_daily_em_df t3
                                           on t1.code = t3.code) t) t
            where rate_1 < 0
              and rate_2 < 0
              and rate_3 < 3
              and rate_4 < 0
              and rate_5 < 0
              and rate_6 > 0) t) t
where success_cnt / fund_cnt >= 0.6
order by date, code
;

-- qtrade3
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
             count(1) over (partition by pattern, code)                       fund_cnt,
             count(if(profit > 0, 1, null)) over (partition by pattern, code) success_cnt
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
where success_cnt / fund_cnt >= 0.83
  and fund_cnt >= 12
  and scale >= 10
order by date desc, scale desc, pattern
;

-- qtrade4
select code,
       name,
       date,
       scale,
       profit,
       pattern,
       success_rate,
       success_cnt,
       fund_cnt
from (select code,
             name,
             date,
             scale,
             profit,
             pattern,
             count(1) over (partition by pattern, code)                       fund_cnt,
             count(if(profit > 0, 1, null)) over (partition by pattern, code) success_cnt,
             success_rate
      from (select t1.code,
                   t1.name,
                   t1.date,
                   t1.scale,
                   t1.close,
                   if(ahead_rate_1 > 0, ahead_rate_1, ahead_rate_2)                                              profit,
                   abs(nvl(t2.pattern1, t1.rate1) - t1.rate1) + abs(nvl(t2.pattern2, t1.rate2) - t1.rate2) +
                   abs(nvl(t2.pattern3, t1.rate3) - t1.rate3) + abs(nvl(t2.pattern4, t1.rate4) - t1.rate4) +
                   abs(nvl(t2.pattern5, t1.rate5) - t1.rate5) + abs(nvl(t2.pattern6, t1.rate6) - t1.rate6) +
                   abs(nvl(t2.pattern7, t1.rate7) - t1.rate7) + abs(nvl(t2.pattern8, t1.rate8) - t1.rate8) +
                   abs(nvl(t2.pattern9, t1.rate9) - t1.rate9) + abs(nvl(t2.pattern10, t1.rate10) - t1.rate10) +
                   abs(nvl(t2.pattern11, t1.rate11) - t1.rate11) + abs(nvl(t2.pattern12, t1.rate12) - t1.rate12) +
                   abs(nvl(t2.pattern13, t1.rate13) - t1.rate13) + abs(nvl(t2.pattern14, t1.rate14) - t1.rate14) +
                   abs(nvl(t2.pattern15, t1.rate15) - t1.rate15) + abs(nvl(t2.pattern16, t1.rate16) - t1.rate16) +
                   abs(nvl(t2.pattern17, t1.rate17) - t1.rate17) + abs(nvl(t2.pattern18, t1.rate18) - t1.rate18) +
                   abs(nvl(t2.pattern19, t1.rate19) - t1.rate19) + abs(nvl(t2.pattern20, t1.rate20) - t1.rate20) +
                   abs(nvl(t2.pattern21, t1.rate21) - t1.rate21) + abs(nvl(t2.pattern22, t1.rate22) - t1.rate22) +
                   abs(nvl(t2.pattern23, t1.rate23) - t1.rate23) + abs(nvl(t2.pattern24, t1.rate24) - t1.rate24) +
                   abs(nvl(t2.pattern25, t1.rate25) - t1.rate25) + abs(nvl(t2.pattern26, t1.rate26) - t1.rate26) +
                   abs(nvl(t2.pattern27, t1.rate27) - t1.rate27) + abs(nvl(t2.pattern28, t1.rate28) - t1.rate28) +
                   abs(nvl(t2.pattern29, t1.rate29) - t1.rate29) + abs(nvl(t2.pattern30, t1.rate30) - t1.rate30) flag,
                   t2.success_rate,
                   concat_ws('', cast(pattern1 as int), cast(pattern2 as int), cast(pattern3 as int),
                             cast(pattern4 as int), cast(pattern5 as int), cast(pattern6 as int), cast(pattern7 as int),
                             cast(pattern8 as int), cast(pattern9 as int), cast(pattern10 as int),
                             cast(pattern11 as int), cast(pattern12 as int), cast(pattern13 as int),
                             cast(pattern14 as int), cast(pattern15 as int), cast(pattern16 as int),
                             cast(pattern17 as int), cast(pattern18 as int), cast(pattern19 as int),
                             cast(pattern20 as int), cast(pattern21 as int), cast(pattern22 as int),
                             cast(pattern23 as int), cast(pattern24 as int), cast(pattern25 as int),
                             cast(pattern26 as int), cast(pattern27 as int), cast(pattern28 as int),
                             cast(pattern29 as int), cast(pattern30 as int), '*')                                pattern
            from (select code,
                         name,
                         date,
                         scale,
                         close,
                         if((close - close_1) > 0, 1, 0)     rate1,
                         if((close_1 - close_2) > 0, 1, 0)   rate2,
                         if((close_2 - close_3) > 0, 1, 0)   rate3,
                         if((close_3 - close_4) > 0, 1, 0)   rate4,
                         if((close_4 - close_5) > 0, 1, 0)   rate5,
                         if((close_5 - close_6) > 0, 1, 0)   rate6,
                         if((close_6 - close_7) > 0, 1, 0)   rate7,
                         if((close_7 - close_8) > 0, 1, 0)   rate8,
                         if((close_8 - close_9) > 0, 1, 0)   rate9,
                         if((close_9 - close_10) > 0, 1, 0)  rate10,
                         if((close_10 - close_11) > 0, 1, 0) rate11,
                         if((close_11 - close_12) > 0, 1, 0) rate12,
                         if((close_12 - close_13) > 0, 1, 0) rate13,
                         if((close_13 - close_14) > 0, 1, 0) rate14,
                         if((close_14 - close_15) > 0, 1, 0) rate15,
                         if((close_15 - close_16) > 0, 1, 0) rate16,
                         if((close_16 - close_17) > 0, 1, 0) rate17,
                         if((close_17 - close_18) > 0, 1, 0) rate18,
                         if((close_18 - close_19) > 0, 1, 0) rate19,
                         if((close_19 - close_20) > 0, 1, 0) rate20,
                         if((close_20 - close_21) > 0, 1, 0) rate21,
                         if((close_21 - close_22) > 0, 1, 0) rate22,
                         if((close_22 - close_23) > 0, 1, 0) rate23,
                         if((close_23 - close_24) > 0, 1, 0) rate24,
                         if((close_24 - close_25) > 0, 1, 0) rate25,
                         if((close_25 - close_26) > 0, 1, 0) rate26,
                         if((close_26 - close_27) > 0, 1, 0) rate27,
                         if((close_27 - close_28) > 0, 1, 0) rate28,
                         if((close_28 - close_29) > 0, 1, 0) rate29,
                         if((close_29 - close_30) > 0, 1, 0) rate30,
                         (ahead_1 - close) / close           ahead_rate_1,
                         (ahead_2 - close) / close           ahead_rate_2
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
                               lag(close, 11) over (partition by t1.code order by date) close_11,
                               lag(close, 12) over (partition by t1.code order by date) close_12,
                               lag(close, 13) over (partition by t1.code order by date) close_13,
                               lag(close, 14) over (partition by t1.code order by date) close_14,
                               lag(close, 15) over (partition by t1.code order by date) close_15,
                               lag(close, 16) over (partition by t1.code order by date) close_16,
                               lag(close, 17) over (partition by t1.code order by date) close_17,
                               lag(close, 18) over (partition by t1.code order by date) close_18,
                               lag(close, 19) over (partition by t1.code order by date) close_19,
                               lag(close, 20) over (partition by t1.code order by date) close_20,
                               lag(close, 21) over (partition by t1.code order by date) close_21,
                               lag(close, 22) over (partition by t1.code order by date) close_22,
                               lag(close, 23) over (partition by t1.code order by date) close_23,
                               lag(close, 24) over (partition by t1.code order by date) close_24,
                               lag(close, 25) over (partition by t1.code order by date) close_25,
                               lag(close, 26) over (partition by t1.code order by date) close_26,
                               lag(close, 27) over (partition by t1.code order by date) close_27,
                               lag(close, 28) over (partition by t1.code order by date) close_28,
                               lag(close, 29) over (partition by t1.code order by date) close_29,
                               lag(close, 30) over (partition by t1.code order by date) close_30,
                               lead(close, 1) over (partition by t1.code order by date) ahead_1,
                               lead(close, 2) over (partition by t1.code order by date) ahead_2
                        from df t1
                                 join scale_df t2
                                      on t1.code = t2.code
                                 left join fund_etf_fund_daily_em_df t3
                                           on t1.code = t3.code) t) t1
                     join stock_patterns t2 on t1.code = t2.code) t
      where flag < 1) t
where scale >= 10
order by date desc, scale desc, pattern
;
