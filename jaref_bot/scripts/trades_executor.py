import argparse
from time import sleep
from datetime import datetime

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name

from jaref_bot.trading.functions import handle_opened_position, handle_close_position, place_market_order



def main(demo):
    if demo:
        print('DEMO mode.')
    else:
        print('========= REAL MONEY mode! =========')

    db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
    postgre_manager = DBManager(db_params)

    redis_orders = RedisManager(db_name = 'orders')
    redis_orderbooks = RedisManager(db_name = 'orderbooks')
    exc_manager = ExchangeManager()
    exc_manager.add_market("bybit_linear", BybitRestAPI('linear'))

    coin_information = exc_manager.get_instrument_data()

    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'{ct} Начинаем работу...')

    while True:
        try:
            pairs = postgre_manager.get_table('pairs')
            pending_orders = redis_orders.get_pending_orders()
            pair_tokens_open = pairs['token_1'].to_list() + pairs['token_2'].to_list()


            if pending_orders:
                for token, data in pending_orders['bybit'].items():
                    side = 'Buy' if data['side'] == 'long' else 'Sell'
                    leverage = int(data['leverage'])
                    price = float(data['price'])
                    dp = int(data['dp'])

                    if token in pair_tokens_open:
                        if data['side'] == 'long':
                            sl = round(price - 0.85 * price / leverage, dp)
                        elif data['side'] == 'short':
                            sl = round(price + 0.85 * price / leverage, dp)

                        resp = place_market_order(demo=demo, exc='bybit_linear', symbol=token,
                            side=side, volume=data['qty'],
                            coin_information=coin_information, stop_loss=sl)

                        handle_opened_position(demo=demo, exc='bybit_linear', symbol=token, order_type='market', coin_information=coin_information)
                    else:
                        resp = place_market_order(demo=demo, exc='bybit_linear', symbol=token,
                            side=side, volume=data['qty'],
                            coin_information=coin_information, stop_loss=None)
                        handle_close_position(demo, resp=resp, exc='bybit_linear', symbol=token, order_type='market',
                                              leverage=leverage, coin_information=coin_information)

                    redis_orders.delete_order('bybit', token)
        except KeyboardInterrupt:
            print('Завершение работы.')
            break
        sleep(0.1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Мониторинг торгового бота")
    parser.add_argument('--demo', action='store_true', help='Включить демонстрационный режим')
    args = parser.parse_args()
    demo = args.demo

    main(demo)
