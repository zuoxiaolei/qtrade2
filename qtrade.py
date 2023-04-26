import re
from datetime import datetime

import joblib
import lightgbm
import numpy as np
import psutil
import pytz
import requests
from sklearn.metrics import classification_report
from StockData import StockData
from QQEmailSender import Mail
import time

import pandas as pd
import akshare as ak
import retrying
from concurrent.futures import ThreadPoolExecutor
import tqdm
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
from scipy.stats import norm

data_path = 'data/stock_data/'
cpu_count = psutil.cpu_count()
windows = 110
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}


@retrying.retry(stop_max_attempt_number=10, stop_max_delay=10000)
def get_fund_scale(code="159819"):
    url = f"https://fund.eastmoney.com/{code}.html"
    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding
    scale = re.findall("基金规模</a>：(.*?)亿元", resp.text)[0].strip()
    return code, float(scale)


def get_fund_scale2(code="159819"):
    try:
        return get_fund_scale(code)
    except:
        None


def get_all_fund_scale():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    codes = fund_etf_fund_daily_em_df["基金代码"].tolist()
    with ThreadPoolExecutor(100) as executor:
        fund_scale = list(tqdm.tqdm(executor.map(get_fund_scale2, codes), total=len(codes)))
    fund_scale = [ele for ele in fund_scale if ele]
    fund_scale = pd.DataFrame(fund_scale, columns=['code', 'scale'])
    fund_scale_old = pd.read_csv("data/dim/scale.csv")
    fund_scale_old["code"] = fund_scale_old["code"].map(str)
    fund_scale_merge = pd.concat([fund_scale, fund_scale_old], axis=0)
    fund_scale_merge = fund_scale_merge.drop_duplicates(subset=["code"])
    fund_scale_merge = fund_scale_merge.sort_values(by="scale", ascending=False)
    fund_scale_merge.to_csv("data/dim/scale.csv", index=False)


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
    scale_df = pd.read_csv("data/dim/scale.csv")
    scale_df["code"] = scale_df["code"].map(str)
    history_data = history_data.merge(scale_df, on="code", how="left")
    history_data.to_csv("data/ads/history_data.csv", index=False)
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
    get_all_fund_scale()
    buy_down_strategy_history(stock_dfs)
    history_df = pd.read_csv("data/ads/history_data.csv")
    scale_df = pd.read_csv("data/dim/scale.csv")
    scale_df["code"] = scale_df["code"].map(str)
    history_df.code = history_df.code.map(str)
    history_df = history_df[history_df.success_rate >= 0.6]
    buy_down_strategy_list = dict(history_df[['code', 'success_rate']].values.tolist())
    success_rate_mapping = dict(history_df[['code', 'success_rate']].values.tolist())
    buy_stocks = []
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    stock_name_map = dict(fund_etf_fund_daily_em_df[['基金代码', '基金简称']].values.tolist())
    for code, df in tqdm.tqdm(list(stock_dfs.items())):
        df["is_up"] = (df['close'] - df['close'].shift(1)) > 0
        if df.tail(5)["is_up"].sum() == 0 and str(code) in buy_down_strategy_list and df.tail(6)["is_up"].sum() != 0:
            buy_stocks.append([code,
                               df.iloc[-1]['date'],
                               stock_name_map[code]])
    string = ""
    if buy_stocks:
        mail = Mail()
        messages = [f"{ele[0]}\t{ele[2]}" for ele in buy_stocks]
        mail.send('\n'.join(messages))
        buy_stocks = pd.DataFrame(buy_stocks, columns=['code', 'date', 'name'])
        buy_stocks = buy_stocks.merge(scale_df, on="code", how='left')
        buy_stocks = buy_stocks.sort_values(by="scale", ascending=False)
        buy_stocks["success_rate"] = buy_stocks["code"].map(success_rate_mapping)
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
