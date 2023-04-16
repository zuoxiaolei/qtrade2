import numpy as np

import pandas as pd
import akshare as ak
import os
import retrying
from concurrent.futures import ThreadPoolExecutor
import easyquotation
import copy
import datetime

import tqdm

data_path = 'data/stock_data/'


class StockData:
    def __init__(self):
        self.prefix = ['sh', 'sz']
        self.update_time = '23'

    def update_date(self):
        fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
        codes = fund_etf_fund_daily_em_df['基金代码'].unique().tolist()

        @retrying.retry(stop_max_attempt_number=5)
        def get_stock_data(code):
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="19900101",
                                         end_date="21000101", adjust="hfq")
                columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
                df = df[columns]
                df.columns = ['date', 'open', 'close', 'high', 'low', 'volume']
                df['code'] = code
                return df
            except Exception as e:
                import traceback
                traceback.print_exc()
                return None

        with ThreadPoolExecutor(100) as executor:
            dfs = list(tqdm.tqdm(executor.map(get_stock_data, codes), total=len(codes)))
            dfs = [ele for ele in dfs if ele is not None]
        dfs = pd.concat(dfs, axis=0)
        dfs = dfs.sort_values(by=['code', 'date'])
        dfs.to_csv("data/fund.csv", index=False)

    def get_stock_data(self, code):
        df = pd.read_csv(os.path.join(data_path, f"{code}.csv"))
        return df

    def get_all_data(self):
        df = pd.read_csv("data/fund.csv", dtype={'code': object})
        quotation = easyquotation.use('sina')
        codes = df["code"].tolist()
        realtime_data = quotation.stocks(codes)
        date = realtime_data[codes[0]]['date']
        if date not in set(df["date"].unique().tolist()):
            tail_df = df.groupby("code").tail(1).copy(deep=True)
            for i in range(len(tail_df)):
                tail_df.iloc[i]['date'] = date
                code = tail_df.iloc[i]['code']
                close = tail_df.iloc[i]['close']
                tail_df.iloc[i][2] = (realtime_data[code]['now'] / realtime_data[code]['close']) * float(close)
            df = pd.concat([df, tail_df], axis=0)
            df = df.sort_values(by=['code', 'date'])
        dfs = {k: v for k, v in df.groupby("code", as_index=False)}
        return dfs

    def get_market_data(self):
        stocks = ak.stock_zh_a_spot_em()
        stock_codes = stocks["代码"]

        def get_market_df(code):
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="hfq")
            if len(stock_zh_a_hist_df) > 0:
                stock_zh_a_hist_df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'increase',
                                              'increase_rate', 'increase_amount', 'exchange_rate']
                stock_zh_a_hist_df['code'] = code
            else:
                stock_zh_a_hist_df = None
            return stock_zh_a_hist_df

        with ThreadPoolExecutor(100) as pool:
            dfs = list(tqdm.tqdm(pool.map(get_market_df, stock_codes), total=len(stock_codes)))
            dfs = [ele for ele in dfs if ele is not None]
        market_df = pd.concat(dfs, axis=0)
        market_df = market_df.sort_values(by=['code', 'date'])
        market_df.to_csv("data/market_df.csv", index=False)


if __name__ == '__main__':
    s = StockData()
    s.update_date()
    s.get_market_data()
