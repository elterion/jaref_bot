from jaref_bot.strategies.arbitrage import find_tokens_to_close_order
from jaref_bot.db.db_manager import DBManager
from config import host, user, password, db_name

import sys
import os
import signal
from IPython.display import clear_output
import pandas as pd
from time import sleep
from datetime import datetime
from prettytable import PrettyTable, TableStyle

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def signal_handler(sig, frame):
    global exit_flag
    print('Завершение работы.')
    exit_flag = True

def clear():
    os.system( 'cls' )

exit_flag = False
signal.signal(signal.SIGINT, signal_handler)

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

if __name__ == '__main__':
    t = PrettyTable(['token', 'long', 'short', 'qty', 'mean', 'std',
                     'diff', 'dev', 'lm_prof', 'sm_prof', 'prof'])
    t.set_style(TableStyle.SINGLE_BORDER)

    while not exit_flag:
        try:
            stats_data = pd.read_parquet('./data/stats_df.parquet')
            db_manager.refresh_current_data(30)
            current_data = db_manager.get_table('current_data')
            current_orders = db_manager.get_table('current_orders')
            history = db_manager.get_table('trading_history')

            open_orders = len(current_orders) // 2
            closed_orders = len(history) // 2

            close_df = find_tokens_to_close_order(current_data,
                                                current_orders,
                                                stats_data,
                                                min_profit = 0.0,
                                                close_edge = -50.0,
                                                std_coef = 0.0,
                                                profit_breakout=0.0)
            clear()
            print(f'{datetime.now().strftime('%H:%M:%S')}, открытых сделок: {open_orders}, закрытых сделок: {closed_orders}')
            t.clear_rows()
            for i, r in close_df.iterrows():
                token = r['token']
                long_exc = r['long_exc']
                short_exc = r['short_exc']
                qty = r['qty'].normalize().to_eng_string()
                diff = round(r['close_diff'], 3)
                mean = round(r['mean'], 3)
                std = round(r['std'], 3)
                dev = round(r['deviation'], 1)
                profit = round(r['profit'], 3)
                lm_profit = round(r['lm_profit'], 3)
                sm_profit = round(r['sm_profit'], 3)

                t.add_row([token, long_exc, short_exc, qty, mean, std, diff, dev, lm_profit, sm_profit, profit])


            # print(type(qty), qty.to_eng_string())
            print(t)
            sleep(2)
        except KeyError:
            print(f"Восстанавливаем соединение...")
            sleep(5)
            continue
        except KeyboardInterrupt:
            print('Завершение работы.')
            sys.exit()
        except RuntimeError as e:
            print(f"Ошибка выполнения: {e}")
