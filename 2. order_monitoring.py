from jaref_bot.strategies.arbitrage import find_tokens_to_close_order
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name

import os
import polars as pl
from time import sleep
from datetime import datetime
from prettytable import PrettyTable, TableStyle

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def clear():
    os.system( 'cls' )

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(db_params)
redis_manager = RedisManager()

if __name__ == '__main__':
    t = PrettyTable(['token', 'long', 'short', 'qty', 'usdt', 'mean', 'std',
                     'diff', 'dev', 'lm_prof', 'sm_prof', 'profit'])
    t.set_style(TableStyle.SINGLE_BORDER)

    while True:
        try:
            stats_data = postgre_manager.get_table('stats_data')
            stats_data = pl.from_pandas(stats_data)
            current_orders = postgre_manager.get_table('current_orders')
            current_orders = pl.from_pandas(current_orders)

            try:
                current_data = redis_manager.get_orderbooks(n_levels=5)
            except pl.exceptions.ColumnNotFoundError:
                sleep(1)
                continue

            history = postgre_manager.get_table('trading_history')

            open_orders = len(current_orders) // 2
            closed_orders = len(history) // 2

            df_out = find_tokens_to_close_order(current_data, current_orders, stats_data,
                                                std_coef=-10,
                                                min_edge=-10)
            clear()
            print(f'{datetime.now().strftime('%H:%M:%S')}, открытых сделок: {open_orders}, закрытых сделок: {closed_orders}')
            t.clear_rows()

            if df_out is None:
                print(t)
                sleep(10)
                continue

            for r in df_out.iter_rows(named=True):
                token = r['token']
                long_exc = r['long_exc']
                short_exc = r['short_exc']

                long_price = current_data.filter((pl.col('symbol') == token) &
                                                 (pl.col('exchange') == long_exc)
                                                 ).select('bidprice_0').item()
                short_price = current_data.filter((pl.col('symbol') == token) &
                                                 (pl.col('exchange') == short_exc)
                                                 ).select('askprice_0').item()
                qty = float(r['qty'])
                diff = round(r['diff'], 3)
                mean = round(r['cmean'], 3)
                std = round(r['cstd'], 3)
                dev = round(r['deviation'], 1)
                profit = round(r['profit'], 2)
                lm_profit = round(r['lm_profit'], 2)
                sm_profit = round(r['sm_profit'], 2)

                usdt = round(max(long_price, short_price) * qty, 2)

                # print(f'{long_price=}; {short_price=}')
                if long_price > short_price:
                    break

                t.add_row([token, long_exc, short_exc, qty, usdt, mean,
                           std, diff, dev, lm_profit, sm_profit, profit])

            print(t)
            sleep(1.0)
        except KeyError:
            print(f"Восстанавливаем соединение...")
            sleep(5)
            continue
        except KeyboardInterrupt:
            print('Завершение работы.')
            break
        except RuntimeError as e:
            print(f"Ошибка выполнения: {e}")
