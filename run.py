from common import tz
import datetime


def run():
    now = datetime.datetime.now(tz)
    if 1 <= now.isoweekday() <= 5 and 9 <= now.hour <= 16:
        import qtrade
        qtrade.run_continue_down_strategy()


if __name__ == '__main__':
    run()
