import argparse
from time import sleep
from datetime import datetime
import polars as pl

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.core.exceptions.trading import PlaceOrderError

from jaref_bot.trading.functions import handle_opened_position, handle_close_position, place_market_order

def find_pair(pairs, in_work):
    in_work_set = set(in_work)
    filtered = pairs.filter(
        pl.col("token_1").is_in(in_work_set) &
        pl.col("token_2").is_in(in_work_set)
    )
    if filtered.height > 0:
        row = filtered.row(0)
        return (row[0], row[1], row[2])
    return None

def main(demo):
    if demo:
        print('DEMO mode.')
    else:
        print('========= REAL MONEY mode! =========')

    db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
    postgre_manager = DBManager(db_params)

    redis_orders = RedisManager(db_name = 'orders')
    redis_sys = RedisManager(db_name = 'system_state')
    exc_manager = ExchangeManager()
    exc_manager.add_market("bybit_linear", BybitRestAPI('linear'))

    coin_information = exc_manager.get_instrument_data()

    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{ct} Начинаем работу...')

    error_counter = 0
    in_work = []

    while error_counter < 5:
        try:
            ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f'Текущее время: {ct}', end='\r')

            # Устанавливаем heartbeat отметку в Redis
            redis_sys.set_system_state('trades_executor', 1)
            pending_orders = redis_orders.get_pending_orders()
            pairs = postgre_manager.get_table('pairs', df_type='polars')

            new_orders = pairs.filter(pl.col('status') == 'created')
            active_orders = pairs.filter(pl.col('status') == 'active')
            closing_orders = pairs.filter(pl.col('status') == 'closing')

            if pending_orders:
                for token, data in pending_orders['bybit'].items():
                    action = data['action']
                    side = data['side']
                    leverage = int(data['leverage'])
                    price = float(data['price'])
                    dp = int(data['dp'])

                    # --- Открываем позицию ---
                    if action == 'open':
                        if side == 'Buy':
                            sl = round(price - 0.85 * price / leverage, dp)
                        elif side == 'Sell':
                            sl = round(price + 0.85 * price / leverage, dp)

                        try:
                            resp = place_market_order(demo=demo, exc='bybit_linear', symbol=token,
                                side=side, volume=data['qty'],
                                coin_information=coin_information, stop_loss=sl)

                            handle_opened_position(demo=demo,
                                                   exc='bybit_linear',
                                                   symbol=token,
                                                   order_type='market',
                                                   coin_information=coin_information)
                            in_work.append(token)
                            error_counter = 0

                            if len(in_work) >=2 :
                                p = find_pair(new_orders, in_work)

                                # Если есть подтверждённая пара
                                if p:
                                    postgre_manager.commit_pair_order(p[0], p[1], p[2])
                                    in_work.remove(p[0])
                                    in_work.remove(p[1])
                                    redis_orders.delete_order('bybit', p[0])
                                    redis_orders.delete_order('bybit', p[1])

                        except PlaceOrderError:
                            error_counter += 1
                            break

                    # --- Закрываем позицию ---
                    else:
                        try:
                            resp = place_market_order(demo=demo, exc='bybit_linear', symbol=token,
                                side=side, volume=data['qty'],
                                coin_information=coin_information, stop_loss=None)
                            handle_close_position(demo, resp=resp, exc='bybit_linear', symbol=token, order_type='market',
                                                leverage=leverage, coin_information=coin_information)
                            error_counter = 0
                            in_work.append(token)

                            if len(in_work) >=2 :
                                p = find_pair(closing_orders, in_work)

                                # Если есть подтверждённая пара
                                if p:
                                    postgre_manager.delete_pair_order(p[0], p[1])
                                    in_work.remove(p[0])
                                    in_work.remove(p[1])
                                    redis_orders.delete_order('bybit', p[0])
                                    redis_orders.delete_order('bybit', p[1])

                        except PlaceOrderError:
                            error_counter += 1
                            break

        except KeyboardInterrupt:
            print('Завершение работы.')
            break
        sleep(0.5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Мониторинг торгового бота")
    parser.add_argument('--demo', action='store_true', help='Включить демонстрационный режим')
    args = parser.parse_args()
    demo = args.demo

    main(demo)
