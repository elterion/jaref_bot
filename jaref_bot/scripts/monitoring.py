from datetime import datetime
from time import sleep
import redis
import logging
import polars as pl
import os
import pickle

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.trading.functions import handle_open_position

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(message)s")
logging.getLogger('aiohttp').setLevel('ERROR')
logging.getLogger('asyncio').setLevel('ERROR')
logger = logging.getLogger()

logger.info('Инициализируем биржи...')
# ====================================
# Инициация нужных криптобирж, рынков и БД
db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(db_params)

redis_price_manager = RedisManager(db_name = 'orderbooks')
redis_order_manager = RedisManager(db_name = 'orders')

with open("./data/coin_information.pkl", "rb") as f:
    coin_information = pickle.load(f)

exit_flag = False

demo = False

while not exit_flag:
    os.system( 'cls' )
    try:
        current_data = redis_price_manager.get_orderbooks(n_levels=5)

        sorted_df = current_data.sort(by='ts')
        oldest_ts_exc = sorted_df.select("exchange").head(1).item()
        last_entry = sorted_df.filter(pl.col('exchange') == oldest_ts_exc).select("ts").tail(1).item()
        time_delta = int(datetime.timestamp(datetime.now())) - last_entry

        # Мониторинг websocket'а и БД Redis
        # =================================
        if time_delta > 20:
            logger.error(f'Отсутствует соединение с биржей {oldest_ts_exc}.')
            sleep(5)
            continue
        else:
            ct = datetime.now().strftime('%H:%M:%S')
            print(f'Последнее обновление ордербуков: {ct}')


        # Мониторинг открытых позиций и ожидающих лимитных ордеров
        # =================================
        current_orders = postgre_manager.get_table('current_orders')
        print(f'Открытых позиций: {len(current_orders)}')

        pending_orders = redis_order_manager.get_pending_orders()
        orders = []

        print(f'\n========== Ожидающие лимитные заявки ===========')
        for exc, data in pending_orders.items():
            for token, td in data.items():
                orders.append(token)
                time = datetime.fromtimestamp(int(td['ts']))
                print(f'{time}: {td['side']} {td['qty']} {token} on {exc} ({td['purpose']} order)')

        for exc, data in pending_orders.items():
            symbol = list(data.keys())[0]
            exc_name = exc+'_linear'
            if handle_open_position(demo=demo,
                                    exc=exc_name,
                                    symbol=symbol,
                                    order_type='limit',
                                    coin_information=coin_information):
                redis_order_manager.delete_order(exchange=exc, token=symbol)

        sleep(1)


    except redis.ConnectionError:
        logger.error(f'Отсутствует соединение с базой данных.')
        sleep(5)
    except KeyboardInterrupt:
        print('Завершение работы.')
        break
