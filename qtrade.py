import re
from datetime import datetime

import joblib
import lightgbm
import numpy as np
import psutil
import pytz
import requests
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

from QQEmailSender import Mail
import time

import pandas as pd
import akshare as ak
import os
import retrying
from concurrent.futures import ThreadPoolExecutor
import easyquotation
import tqdm
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql import SparkSession
from scipy.stats import norm

data_path = 'data/stock_data/'
cpu_count = psutil.cpu_count()
windows = 110
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}


def load_spark_sql():
    sql_text = open('data/spark.sql', encoding='utf-8').read()
    spark_sql = [ele for ele in sql_text.split(";") if ele]
    return spark_sql


spark_sql = load_spark_sql()


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


class StockData:
    def __init__(self):
        self.prefix = ['sh', 'sz']
        self.update_time = '23'
        self.date = None

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
        dfs.to_csv("data/ods/fund.csv", index=False)

    def get_stock_data(self, code):
        df = pd.read_csv(os.path.join(data_path, f"{code}.csv"))
        return df

    def get_all_data(self):
        df = pd.read_csv("data/ods/fund.csv", dtype={'code': object})
        quotation = easyquotation.use('sina')
        codes = df["code"].tolist()
        realtime_data = quotation.stocks(codes)
        date = realtime_data[codes[0]]['date']
        self.date = date
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


def get_spark():
    parallel_num = str(cpu_count * 3)
    spark = SparkSession.builder \
        .appName("chain merge") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", parallel_num) \
        .config("spark.default.parallelism", parallel_num) \
        .config("spark.ui.showConsoleProgress", True) \
        .config("spark.executor.memory", '1g') \
        .config("spark.driver.memory", '2g') \
        .config("spark.driver.maxResultSize", '2g') \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executor.extraJavaOptions", "-Xss1024M") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_predict_udf(close, array):
    if len(array) < 2:
        return None
    buy = [1 for ele in array if ele > close]
    return 1 if buy else 0


def get_cdf_udf(close, mean, std):
    if isinstance(close, float) and isinstance(mean, float) and isinstance(std, float):
        return float(norm.cdf(close, loc=mean, scale=std))
    else:
        return None


def get_feature(stock_dfs, is_train=True):
    start_time = time.time()
    spark = get_spark()
    select_dfs = []
    for code, df in list(stock_dfs.items()):
        if len(df) > 300:
            df["code"] = code
            select_dfs.append(df)
    df_origin = pd.concat(select_dfs, axis=0)
    date = df_origin["date"].max()
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    stock_name_map = dict(fund_etf_fund_daily_em_df[['基金代码', '基金简称']].values.tolist())
    df_origin["code"] = df_origin["code"].map(str)
    df_origin["name"] = df_origin["code"].map(stock_name_map)
    df_origin = df_origin.sort_values(by=['code', 'date'])
    schema = StructType([StructField("date", StringType(), True),
                         StructField("open", DoubleType(), True),
                         StructField("close", DoubleType(), True),
                         StructField("high", DoubleType(), True),
                         StructField("low", DoubleType(), True),
                         StructField("volume", IntegerType(), True),
                         StructField("code", StringType(), True),
                         StructField("name", StringType(), True)
                         ])
    df_origin_rdd = spark.createDataFrame(df_origin, schema=schema)
    ts_behind_feature_names = [f'lag(close, {i}) over(partition by code order by date) close_{i},' for i in
                               range(windows)]
    ts_behind_feature_names = '\n'.join(ts_behind_feature_names)
    spark.udf.register("get_cdf", get_cdf_udf, DoubleType())
    spark.udf.register("get_predict_udf", get_predict_udf, IntegerType())
    df_origin_rdd.createOrReplaceTempView("df_origin_rdd")
    condition = "1=1" if is_train else f"date='{date}'"
    sql = spark_sql[0].format(windows,
                              windows,
                              ts_behind_feature_names,
                              1, condition)
    print(sql)
    df = spark.sql(sql)
    df_train = df.where((df["y"].isNotNull()) & (df["close_109"].isNotNull()))
    df_predict = df.where(df["close_109"].isNotNull())
    feature_cols = list(df_train.columns)
    for col in ["code", "name", "date", "close", "y"]:
        feature_cols.remove(col)
    if is_train:
        df_train = df_train.toPandas()
    else:
        df_train = df_predict.toPandas()
    print(f"get_feature cost {time.time() - start_time}! shape: {df_train.shape}")
    spark.stop()
    return df_train, feature_cols


def train_lightgbm():
    start_time = time.time()
    s = StockData()
    stock_dfs = s.get_all_data()
    df_train, feature_cols, = get_feature(stock_dfs)
    df_train_ = df_train[df_train.date < "2022-01-01"]
    df_test = df_train[(df_train.date >= "2022-01-01") & (df_train.date < "2024-01-01")]
    # df_valid = df_train[(df_train.date >= "2023-01-01")]
    # x = df_train[feature_cols]
    # y = df_train["y"]
    X_train, X_test = df_train_[feature_cols], df_test[feature_cols]
    y_train, y_test = df_train_["y"], df_test["y"]
    model = lightgbm.LGBMClassifier(num_leaves=1000,
                                    n_estimators=100000)
    model.fit(X_train, y_train,
              eval_set=[(X_train, y_train), (X_test, y_test)],
              early_stopping_rounds=50)
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred))
    joblib.dump(model, "data/model20230421.pkl", compress=9)
    end_time = time.time()
    print(end_time - start_time)


def evaluate():
    stock_dfs = StockData().get_all_data()
    get_all_fund_scale()
    dfs, col_names = get_feature(stock_dfs, is_train=False)
    x = dfs[col_names]
    model = joblib.load("data/model.pkl")
    y_pred_prob = model.predict_proba(x)
    y_pred = np.argmax(y_pred_prob, axis=1)
    dfs['y_pred'] = y_pred
    dfs['y_pred_prob'] = y_pred_prob[:, 1]
    dfs = dfs["code,name,date,close,cdf,y,y_pred,y_pred_prob".split(",")]
    dfs = dfs[(dfs["y_pred_prob"] >= 0.9) & (dfs["y_pred"] == 1) & (dfs["cdf"] <= 0.5)]
    string = ""
    if len(dfs) > 0:
        df_scale = pd.read_csv("data/dim/scale.csv")
        df_scale["code"] = df_scale["code"].map(str)
        dfs["code"] = dfs["code"].map(str)
        dfs = dfs.merge(df_scale, on="code", how="left")
        dfs = dfs.sort_values(by="scale", ascending=False)
        date = dfs["date"].max()
        dfs.to_csv(f"data/ads/{date}.csv", index=False)
        dfs = dfs.drop("y", axis=1)
        title = "## 机器学习预测涨跌"
        string = write_table(title, dfs.columns.tolist(), dfs)
    return string


if __name__ == '__main__':
    run_continue_down_strategy()
