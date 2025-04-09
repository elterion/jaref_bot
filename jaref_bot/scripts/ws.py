import asyncio
from asyncio.exceptions import CancelledError
import logging
import redis
import polars as pl
from datetime import datetime

from jaref_bot.core.exchange.bybit_websocket import BybitWebSocketClient
from jaref_bot.core.exchange.okx_websocket import OkxWebSocketClient
from jaref_bot.core.exchange.gate_websocket import GateWebSocketClient

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name

from jaref_bot.strategies.arbitrage import get_best_prices

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(db_params)
redis_manager = RedisManager('orderbooks')

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


async def copy_data_from_redis():
    while True:
        try:
            tdf = redis_manager.get_orderbooks(n_levels=5)
            last_update = tdf.select('ts').max().item()
            time_delta = int(datetime.timestamp(datetime.now())) - last_update

            if time_delta > 5:
                # print('Данные давно не обновлялись.')
                await asyncio.sleep(1)
                continue

            bp = get_best_prices(tdf)
            postgre_manager.copy_data_from_redis(bp)
            await asyncio.sleep(1)
        except pl.exceptions.ColumnNotFoundError:
            await asyncio.sleep(10)
        except KeyboardInterrupt:
            break


async def manage_subscriptions(bybit_client=None, okx_client=None, gate_client=None, token_list=None):
    await bybit_client.subscribe_orderbook_stream(depth=50, tickers=token_list)
    await okx_client.subscribe_orderbook_stream(depth=5, tickers=token_list)
    await gate_client.subscribe_orderbook_stream(depth=5, tickers=token_list)
    # await bybit_client.subscribe_order_stream()
    # await okx_client.subscribe_order_stream()


async def main(demo=False, token_list=None):
    bybit_client = BybitWebSocketClient(demo=demo)
    okx_client = OkxWebSocketClient(demo=demo)
    gate_client = GateWebSocketClient(demo=demo)

    tasks = [
        bybit_client.connect("orderbook", auth_required=False),
        # bybit_client.connect("order", auth_required=True),
        okx_client.connect("orderbook", auth_required=False),
        # okx_client.connect("order", auth_required=True),
        gate_client.connect("orderbook", auth_required=False),

        manage_subscriptions(bybit_client=bybit_client,
                             okx_client=okx_client,
                             gate_client=gate_client,
                             token_list=token_list),
        copy_data_from_redis()
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    token_list = ['ADA_USDT', 'AI_USDT', 'DYM_USDT', 'MANTA_USDT', 'NEAR_USDT', 'ONT_USDT',
                  'OP_USDT', 'PIXEL_USDT', 'RVN_USDT', 'SAND_USDT', 'STRK_USDT', 'T_USDT',
                  'TIA_USDT', 'VANA_USDT', 'W_USDT', 'XAI_USDT']
    db_num = {'orderbooks': 0, 'prices': 1, 'orders': 2}

    try:
        redis_client = redis.Redis(db=db_num['orderbooks'], decode_responses=True)
        print('Redis connected:', redis_client.ping())
        redis_client.flushdb()
        print('Очищаем таблицу')

        asyncio.run(main(demo=False, token_list=token_list))
    except (KeyboardInterrupt, CancelledError):
        print('Завершение работы.')
