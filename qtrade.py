from datetime import datetime
import psutil
import pytz
import time
import pandas as pd
from pyspark.sql import SparkSession

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
    stock_df_filename = qdata_prefix + "ads/exchang_fund_rt.csv"
    scale_df_filename = qdata_prefix + "dim/scale.csv"
    fund_etf_fund_daily_em_df_filename = qdata_prefix + "dim/exchang_eft_basic_info.csv"
    if is_local:
        stock_df_filename = "data/ads/exchang_fund_rt.csv"
        scale_df_filename = "data/dim/scale.csv"
        fund_etf_fund_daily_em_df_filename = "data/dim/exchang_eft_basic_info.csv"

    stock_df = pd.read_csv(stock_df_filename, dtype={"code": object})
    scale_df = pd.read_csv(scale_df_filename, dtype={"code": object})
    fund_etf_fund_daily_em_df = pd.read_csv(fund_etf_fund_daily_em_df_filename, dtype={'基金代码': object})
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称']]
    fund_etf_fund_daily_em_df.columns = ['code', 'name']

    spark = get_spark()
    df = spark.createDataFrame(stock_df)
    scale_df = spark.createDataFrame(scale_df)
    fund_etf_fund_daily_em_df = spark.createDataFrame(fund_etf_fund_daily_em_df)
    df.createOrReplaceTempView("df")
    scale_df.createOrReplaceTempView("scale_df")
    fund_etf_fund_daily_em_df.createOrReplaceTempView("fund_etf_fund_daily_em_df")
    result = spark.sql(spark_sql[0])
    buy_stocks = result.toPandas()
    buy_stocks = buy_stocks.sort_values(["code", "date"])
    buy_stocks.to_csv("data/ads/history_data.csv", index=False)
    buy_stocks = buy_stocks[buy_stocks.date == buy_stocks.date.max()]
    spark.stop()

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
