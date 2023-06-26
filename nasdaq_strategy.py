import akshare as ak
from qtrade import send_message, load_md5, get_string_md5
import pytz
from datetime import datetime

symbol = '100.NDX'
tokens = ['b0b21688d4694f7999c301386ee90a0c',  # xiaolei
          # '4b4b075475bc41e8a39704008677010f',  # peilin
          '44b351689de6492ab519a923e4c202da',  # jiayu
          ]


def nasdaq_strategy():
    stock_us_hist_df = ak.stock_us_hist(symbol=symbol,
                                        period="weekly",
                                        start_date="19700101",
                                        end_date="22220101",
                                        adjust="hfq")
    stock_us_hist_df_daily = ak.stock_us_hist(symbol=symbol,
                                              period="daily",
                                              start_date="19700101",
                                              end_date="22220101",
                                              adjust="hfq")

    stock_us_hist_df = stock_us_hist_df[['日期', '收盘', '振幅']]
    stock_us_hist_df['mean5'] = stock_us_hist_df['收盘'].rolling(5).mean()
    stock_us_hist_df['mean10'] = stock_us_hist_df['收盘'].rolling(10).mean()
    stock_us_hist_df['is_buy'] = (stock_us_hist_df['mean5'] - stock_us_hist_df['mean10']) > 0
    stock_us_hist_df['is_buy'] = stock_us_hist_df['is_buy'] - 0

    is_buy = stock_us_hist_df.iloc[-1]['is_buy']
    date = stock_us_hist_df_daily.iloc[-1]['日期']
    increase_rate = stock_us_hist_df_daily.iloc[-1]['收盘'] / stock_us_hist_df_daily.iloc[-2]['收盘'] - 1
    tips = 'buy' if is_buy == 1 else 'hold'
    info = {'code': symbol,
            'name': '纳斯达克100指数',
            'date': date,
            'increase_rate': str(round(increase_rate * 100, 2)) + "%",
            'fix_inestment_tips': tips
            }
    is_gold_cross = 0
    if int(stock_us_hist_df.iloc[-1]['is_buy']) == 1 and int(stock_us_hist_df.iloc[-2]['is_buy']) == 0:
        is_gold_cross = 1
    if is_gold_cross == 1:
        info['gold_cross'] = '黄金交叉点'
    md5_string = get_string_md5(str(info))
    old_md5_string = load_md5("data/nasdaq_md5.txt")
    if md5_string != old_md5_string:
        with open("data/nasdaq_md5.txt", 'w', encoding='utf-8') as fh:
            fh.write(md5_string)
        content = ''
        for key, value in info.items():
            content = content + f"{key}: {value}\n"
        for token in tokens:
            print(content)
            send_message(content, token)


if __name__ == '__main__':
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    if 9 <= now.hour <= 15:
        nasdaq_strategy()
