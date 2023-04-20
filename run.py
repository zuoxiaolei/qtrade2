import pytz
import datetime
import qtrade
tz = pytz.timezone('Asia/Shanghai')


def run():
    now = datetime.datetime.now(tz)
    if 1 <= now.isoweekday() <= 5 and 9 <= now.hour <= 16:
        qtrade.run_continue_down_strategy()


if __name__ == '__main__':
    run()
