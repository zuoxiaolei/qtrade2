from datetime import datetime

import pytz

from QQEmailSender import Mail
import time

import pandas as pd
import akshare as ak
import os
import retrying
from concurrent.futures import ThreadPoolExecutor
import easyquotation
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


def cal_continue_down(df, cnt=5, future=1):
    result = []

    i = 0
    down_count = 0
    while i < len(df) - 5:
        down_count = 0 if df.iloc[i]['is_up'] else down_count + 1
        if down_count == cnt:
            increase_rates = [(df.iloc[i + ele + 1]['date'], df.iloc[i + ele + 1]['close'] / df.iloc[i]['close'] - 1)
                              for ele in range(future)]
            select_data = [ele for ele in increase_rates if ele[1] > 0]
            if not select_data:
                result.append(increase_rates[-1])
            else:
                result.append(select_data[0])
        i += 1
    return result


def buy_down_strategy_history(stock_dfs):
    history_data = []
    for code, df in tqdm.tqdm(list(stock_dfs.items())):
        df["is_up"] = (df['close'] - df['close'].shift(1)) > 0
        result = cal_continue_down(df, cnt=5, future=2)
        if result:
            history_data.extend([(code, date, increase_rate) for date, increase_rate in result])

    history_data = pd.DataFrame(history_data, columns=['code', 'date', 'increase_rate'])
    history_data["success_rate"] = history_data.groupby("code")["increase_rate"].transform(
        lambda x: (x > 0).sum() / len(x))
    history_data = history_data.sort_values(by=['code', 'date'])
    history_data.to_csv("data/history_data.csv", index=False)
    return history_data


def write_table(title, columns, df):
    string = ""
    string += title + "\n"
    table_header = ' | ' + ' | '.join(columns) + ' | '
    table_header += "\n" + ' | ' + ' | '.join([":-----" for _ in range(len(columns))]) + ' | '
    for i in range(len(df)):
        row = df.iloc[i]
        row = [str(ele) for ele in list(row)]
        table_header += "\n" + ' | ' + ' | '.join(row) + ' | '
    string += table_header + "\n"
    return string


def run_continue_down_strategy():
    start_time = time.time()
    s = StockData()
    s.update_date()
    stock_dfs = s.get_all_data()
    buy_down_strategy_history(stock_dfs)
    history_df = pd.read_csv("data/history_data.csv")
    history_df.code = history_df.code.map(str)
    history_df = history_df[history_df.success_rate >= 0.6]
    buy_down_strategy_list = dict(history_df[['code', 'success_rate']].values.tolist())
    buy_stocks = []
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    stock_name_map = dict(fund_etf_fund_daily_em_df[['基金代码', '基金简称']].values.tolist())

    for code, df in tqdm.tqdm(list(stock_dfs.items())):
        df["is_up"] = (df['close'] - df['close'].shift(1)) > 0
        df = df.tail(5)
        if df["is_up"].sum() == 0 and str(code) in buy_down_strategy_list:
            buy_stocks.append([code,
                               df.iloc[-1]['date'],
                               stock_name_map[code]])
    if buy_stocks:
        mail = Mail()
        messages = [f"{ele[0]}\t{ele[1]}" for ele in buy_stocks]
        mail.send('\n'.join(messages))
        buy_stocks = pd.DataFrame(buy_stocks, columns=['code', 'date', 'name'])
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz).strftime("%Y%m%d")
        title = "# {}量化交易报告".format(now)
        string = write_table(title, buy_stocks.columns.tolist(), buy_stocks)
        with open("README.md", "w", encoding="utf-8") as fh:
            fh.write(string)
    end_time = time.time()
    print(end_time - start_time)


if __name__ == '__main__':
    run_continue_down_strategy()
