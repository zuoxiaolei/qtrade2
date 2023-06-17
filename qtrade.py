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
import hashlib
import requests

cpu_count = psutil.cpu_count()
windows = 110
qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"
sequence_length = 10
seq_length = 30
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y%m%d")


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


spark = get_spark()


def get_file_md5(file_name):
    """
    计算文件的md5
    :param file_name:
    :return:
    """
    m = hashlib.md5()  # 创建md5对象
    with open(file_name, 'rb') as fobj:
        while True:
            data = fobj.read(4096)
            if not data:
                break
            m.update(data)  # 更新md5对象
    return m.hexdigest()


def load_md5(filename):
    with open(filename, 'r', encoding='utf-8') as fh:
        md5_string = fh.read()
        return md5_string.strip()


tokens = ['b0b21688d4694f7999c301386ee90a0c',
          '4b4b075475bc41e8a39704008677010f',  # peilin
          ]


def send_message(content, token):
    params = {
        'token': token,
        'title': now,
        'content': content,
        'template': 'txt'}
    url = 'http://www.pushplus.plus/send'
    res = requests.get(url, params=params)
    print(res)


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


def get_pattern():
    '''生成涨跌的pattern'''
    patterns = []
    for i in range(1, sequence_length + 1):
        total = 2 ** i
        for j in range(total):
            temp = bin(j).replace("0b", "")
            temp = [int(ele) for ele in temp]
            if len(temp) == i:
                temp = [None] * (sequence_length - len(temp)) + temp
                patterns.append(temp[::-1])
    return patterns


def run_qtrade3(is_local=False):
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
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称', '类型']]
    fund_etf_fund_daily_em_df.columns = ['code', 'name', 'type']

    df = spark.createDataFrame(stock_df)
    scale_df = spark.createDataFrame(scale_df)
    fund_etf_fund_daily_em_df = spark.createDataFrame(fund_etf_fund_daily_em_df)
    df.createOrReplaceTempView("df")
    scale_df.createOrReplaceTempView("scale_df")
    fund_etf_fund_daily_em_df.createOrReplaceTempView("fund_etf_fund_daily_em_df")
    pattern = get_pattern()
    columns = [f"pattern{ele + 1}" for ele in range(sequence_length)]
    pattern = spark.createDataFrame(pattern, schema=columns)
    pattern.createOrReplaceTempView("pattern")
    result = spark.sql(spark_sql[2])
    result_df = result.toPandas()
    result_df.to_csv("data/ads/history_data2.csv")
    result_df = result_df[result_df.date == result_df.date.max()]
    result_df.to_csv("data/ads/history_data_latest.csv", index=False)
    md5_string = get_file_md5("data/ads/history_data_latest.csv")
    old_md5_string = load_md5("data/md5.txt")
    print(old_md5_string)
    print(md5_string)
    if md5_string != old_md5_string:
        content = ''
        cols = 'code,name,date,scale,pattern,success_rate,success_cnt,fund_cnt'.split(',')
        for index, row in result_df.iterrows():
            for col in cols:
                content = content + f"{col}: {row[col]}\n"
            content += "\n"
        for token in tokens:
            send_message(content, token)
        with open("data/md5.txt", 'w', encoding='utf-8') as fh:
            fh.write(md5_string)
    string = ""
    if len(result_df):
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz).strftime("%Y%m%d")
        title = "# {} qtrade3".format(now)
        string = write_table(title, result_df.columns.tolist(), result_df)
    return string


def run_down_buy_strategy(is_local=False):
    string = ""
    qtrade3_result = run_qtrade3(is_local)
    with open("qtrade.md", "w", encoding="utf-8") as fh:
        fh.write(string + "\n" + qtrade3_result)


def get_profit():
    df = pd.read_csv("data/ads/history_data2.csv")
    df = df.sort_values(by=["date", "scale"], ascending=[False, False])
    df = df.groupby("date").head(1)
    df["month"] = df["date"].map(lambda x: x[:7])
    profit = df.groupby("month")["profit"].sum()
    print(profit.tail(20))
    return profit[-1]


def get_train_pattern(increase_flag, flag):
    data = {}
    result = []
    for i in range(seq_length, len(increase_flag)):
        for j in range(1, seq_length):
            pattern = ''.join(increase_flag[(i - j):i][::-1])
            if pattern not in data:
                data[pattern] = [flag[i - 1]]
            else:
                data[pattern].append(flag[i - 1])
    for k, v in data.items():
        rate = len([ele for ele in v if ele > 0]) / len(v)
        if len(v) >= 12 and rate >= 0.83:
            result.append([k, rate])
    return result


def get_train_result(df_full):
    train_data = []
    codes = df_full.code.unique().tolist()
    for code in codes:
        df = df_full[df_full.code == code]
        df.loc[:, 'increase_rate'] = df.loc[:, 'close'] / df.loc[:, 'close'].shift(1) - 1
        df.loc[:, 'increase_flag'] = df.loc[:, 'increase_rate'].map(lambda x: 1 if x > 0 else 0).map(str)
        df.loc[:, 'ahead1'] = df.loc[:, 'close'].shift(-1) / df.loc[:, 'close'] - 1
        df.loc[:, 'ahead2'] = df.loc[:, 'close'].shift(-2) / df.loc[:, 'close'] - 1
        df.loc[:, 'flag'] = df.apply(lambda x: 1 if (x['ahead1'] if x['ahead1'] > 0 else x['ahead2']) > 0 else 0,
                                     axis=1)
        df = df.dropna()
        increase_flag = df.loc[:, 'increase_flag'].tolist()
        flag = df['flag'].tolist()
        patterns_rate = get_train_pattern(increase_flag, flag)
        for pattern, rate in patterns_rate:
            pattern = [int(ele) for ele in pattern] + [None] * (seq_length - len(pattern))
            train_data.append([code, *pattern, rate])
    columns = [f"pattern{ele + 1}" for ele in range(seq_length)]
    train_data = pd.DataFrame(train_data, columns=['code', *columns, 'success_rate'])
    return train_data


def run_qtrade4():
    df = pd.read_csv("data/ads/exchang_fund_rt.csv", dtype={'code': object})
    stock_patterns = get_train_result(df)
    stock_df_filename = "data/ads/exchang_fund_rt.csv"
    scale_df_filename = "data/dim/scale.csv"
    fund_etf_fund_daily_em_df_filename = "data/dim/exchang_eft_basic_info.csv"
    stock_df = pd.read_csv(stock_df_filename, dtype={"code": object})
    scale_df = pd.read_csv(scale_df_filename, dtype={"code": object})
    fund_etf_fund_daily_em_df = pd.read_csv(fund_etf_fund_daily_em_df_filename, dtype={'基金代码': object})
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称']]
    fund_etf_fund_daily_em_df.columns = ['code', 'name']

    stock_patterns = spark.createDataFrame(stock_patterns)
    df = spark.createDataFrame(stock_df)
    scale_df = spark.createDataFrame(scale_df)
    fund_etf_fund_daily_em_df = spark.createDataFrame(fund_etf_fund_daily_em_df)

    df.createOrReplaceTempView("df")
    scale_df.createOrReplaceTempView("scale_df")
    fund_etf_fund_daily_em_df.createOrReplaceTempView("fund_etf_fund_daily_em_df")
    stock_patterns.createOrReplaceTempView("stock_patterns")

    result = spark.sql(spark_sql[3])
    result_df = result.toPandas()
    result_df.to_csv("data/ads/history_data4.csv", index=False)
    result_df = result_df[result_df.date == result_df.date.max()]
    string = ""
    if len(result_df):
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz).strftime("%Y%m%d")
        title = "# {} qtrade4".format(now)
        string = write_table(title, result_df.columns.tolist(), result_df)
    print(string)


def main(is_local=False):
    start_time = time.time()
    run_down_buy_strategy(is_local)
    print(f"run_down_buy_strategy cost {time.time() - start_time} second")


if __name__ == '__main__':
    main(is_local=False)
