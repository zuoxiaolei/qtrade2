from datetime import datetime
import psutil
import pytz
import time
import pandas as pd
import tqdm

cpu_count = psutil.cpu_count()
windows = 110
qdata_prefix = "https://raw.githubusercontent.com/zuoxiaolei/qdata/main/data/"
github_proxy_prefix = "https://ghproxy.com/"


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


def buy_down_strategy_history(stock_dfs, scale_df):
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
    stock_df = pd.read_csv(qdata_prefix + "ads/exchang_fund_rt.csv", dtype={"code": object})
    history_df = pd.read_csv("data/ads/history_data.csv", dtype={"code": object})
    scale_df = pd.read_csv(qdata_prefix + "dim/scale.csv", dtype={"code": object})
    stock_dfs = {k: v for k, v in stock_df.groupby("code", as_index=False)}
    buy_down_strategy_history(stock_dfs, scale_df)
    history_df = history_df[history_df.success_rate >= 0.6]
    buy_down_strategy_list = dict(history_df[['code', 'success_rate']].values.tolist())
    success_rate_mapping = dict(history_df[['code', 'success_rate']].values.tolist())
    buy_stocks = []
    fund_etf_fund_daily_em_df = pd.read_csv(qdata_prefix + "dim/exchang_eft_basic_info.csv", dtype={'基金代码': object})
    stock_name_map = dict(fund_etf_fund_daily_em_df[['基金代码', '基金简称']].values.tolist())
    for code, df in tqdm.tqdm(list(stock_dfs.items())):
        df["is_up"] = (df['close'] - df['close'].shift(1)) > 0
        if df.tail(5)["is_up"].sum() == 0 and str(code) in buy_down_strategy_list and df.tail(6)["is_up"].sum() != 0:
            buy_stocks.append([code,
                               df.iloc[-1]['date'],
                               stock_name_map[code]])
    string = ""
    if buy_stocks:
        buy_stocks = pd.DataFrame(buy_stocks, columns=['code', 'date', 'name'])
        buy_stocks = buy_stocks.merge(scale_df, on="code", how='left')
        buy_stocks = buy_stocks.sort_values(by="scale", ascending=False)
        buy_stocks["success_rate"] = buy_stocks["code"].map(success_rate_mapping)
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz).strftime("%Y%m%d")
        title = "# {}".format(now)
        string = write_table(title, buy_stocks.columns.tolist(), buy_stocks)

    with open("qtrade.md", "w", encoding="utf-8") as fh:
        fh.write(string)
    end_time = time.time()
    print(end_time - start_time)


if __name__ == '__main__':
    start_time = time.time()
    run_continue_down_strategy()
    print(f"qtrade cost {time.time() - start_time} second")
