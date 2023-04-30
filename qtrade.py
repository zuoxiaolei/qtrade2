import random
from datetime import datetime

import joblib
import lightgbm
import psutil
import pytz
import time
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.metrics import classification_report

cpu_count = psutil.cpu_count()
windows = 110
qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"


def load_spark_sql():
    sql_text = open('data/spark.sql', encoding='utf-8').read()
    spark_sql = [ele for ele in sql_text.split(";") if ele]
    return spark_sql


spark_sql = load_spark_sql()


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


def process_data(is_local=False):
    stock_df_filename = qdata_prefix + "ads/exchang_fund_rt.csv"
    scale_df_filename = qdata_prefix + "dim/scale.csv"
    fund_etf_fund_daily_em_df_filename = qdata_prefix + "dim/exchang_eft_basic_info.csv"
    stock_cnt_filename = qdata_prefix + "ads/stock_cnt_rt.csv"
    if is_local:
        stock_df_filename = "data/ads/exchang_fund_rt.csv"
        scale_df_filename = "data/dim/scale.csv"
        fund_etf_fund_daily_em_df_filename = "data/dim/exchang_eft_basic_info.csv"
        stock_cnt_filename = "data/ads/stock_cnt_rt.csv"
    stock_df = pd.read_csv(stock_df_filename, dtype={"code": object})
    scale_df = pd.read_csv(scale_df_filename, dtype={"code": object})
    fund_etf_fund_daily_em_df = pd.read_csv(fund_etf_fund_daily_em_df_filename, dtype={'基金代码': object})
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称']]
    fund_etf_fund_daily_em_df.columns = ['code', 'name']
    stock_cnt = pd.read_csv(stock_cnt_filename, dtype={"code": object})

    spark = get_spark()
    df = spark.createDataFrame(stock_df)
    scale_df = spark.createDataFrame(scale_df)
    fund_etf_fund_daily_em_df = spark.createDataFrame(fund_etf_fund_daily_em_df)
    df.createOrReplaceTempView("df")
    scale_df.createOrReplaceTempView("scale_df")
    fund_etf_fund_daily_em_df.createOrReplaceTempView("fund_etf_fund_daily_em_df")
    result = spark.sql(spark_sql[1])
    stock_cnt["increase_rate"] = stock_cnt["increase_cnt"] / stock_cnt["decrease_cnt"]
    result_df = result.toPandas()
    result_df = result_df.merge(stock_cnt, on=['date'])
    return result_df


def train_lightgbm(is_local=False):
    result_df = process_data(is_local=is_local)
    result_df.index = range(len(result_df))
    index = list(range(len(result_df)))
    random.shuffle(index)
    num = int(0.7 * len(index))
    train_index = index[:num]
    test_index = index[num:]
    featurs_col = ['scale', 'success_rate', 'rate_1',
                   'rate_2', 'rate_3', 'rate_4', 'rate_5', 'rate_6', 'code_cnt', 'rate_sum', 'increase_cnt',
                   'decrease_cnt', 'increase_rate']
    result_df["label"] = result_df['profit'].map(lambda x: 1 if x > 0 else 0)
    train_df, test_df = result_df.loc[train_index], result_df.loc[test_index]
    model = lightgbm.LGBMClassifier(num_leaves=1000,
                                    n_estimators=100000)
    model.fit(train_df[featurs_col], train_df["label"],
              eval_set=[(train_df[featurs_col], train_df["label"]), (test_df[featurs_col], test_df["label"])],
              early_stopping_rounds=50)
    y_pred = model.predict(test_df[featurs_col])
    print(classification_report(test_df["label"], y_pred))
    joblib.dump(model, "data/stock_lightgbm.model")


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


def run_down_buy_strategy(is_local=False):
    buy_stocks = process_data(is_local=is_local)
    buy_stocks = buy_stocks.sort_values(["code", "date"])
    model = joblib.load("data/stock_lightgbm.model")
    featurs_col = ['scale', 'success_rate', 'rate_1',
                   'rate_2', 'rate_3', 'rate_4', 'rate_5', 'rate_6', 'code_cnt', 'rate_sum', 'increase_cnt',
                   'decrease_cnt', 'increase_rate']
    buy_stocks["pred"] = model.predict(buy_stocks[featurs_col])
    buy_stocks = buy_stocks["code,name,date,scale,profit,success_rate,pred".split(",")]
    buy_stocks.to_csv("data/ads/history_data.csv", index=False)
    buy_stocks = buy_stocks[buy_stocks.date == buy_stocks.date.max()]

    string = ""
    if len(buy_stocks):
        buy_stocks = buy_stocks.sort_values(by="scale", ascending=False)
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz).strftime("%Y%m%d")
        title = "# {}".format(now)
        string = write_table(title, buy_stocks.columns.tolist(), buy_stocks)

    with open("qtrade.md", "w", encoding="utf-8") as fh:
        fh.write(string)


if __name__ == '__main__':
    start_time = time.time()
    run_down_buy_strategy()
    print(f"run_down_buy_strategy cost {time.time() - start_time} second")
