import asyncio
from asyncio.exceptions import CancelledError
import logging
import redis
from time import sleep
from jaref_bot.core.exchange.bybit_websocket import BybitWebSocketClient
from jaref_bot.core.exceptions.connection import ConnectionLostError

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

async def manage_subscriptions(bybit_client=None, okx_client=None, gate_client=None, token_list=None):
    await bybit_client.subscribe_orderbook_stream(depth=50, tickers=token_list)

async def main(demo=False, token_list=None):
    bybit_client = BybitWebSocketClient(demo=demo)

    tasks = [
        bybit_client.connect("orderbook", auth_required=False),

        manage_subscriptions(bybit_client=bybit_client,
                             token_list=token_list),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    token_list = ['AKT_USDT', 'APT_USDT', 'ARB_USDT', 'ARKM_USDT',
                  'C98_USDT',  'CELO_USDT', 'CHR_USDT', 'CHZ_USDT',
                  'ENJ_USDT', 'FIL_USDT', 'FLOW_USDT',
                  'GALA_USDT', 'GMT_USDT', 'GRT_USDT', 'GTC_USDT',
                  'MANA_USDT',
                  'OGN_USDT', 'ONDO_USDT', 'ONG_USDT', 'OP_USDT',
                  'PHA_USDT', 'ROSE_USDT',
                  'SAND_USDT', 'STG_USDT', 'SNX_USDT', 'VET_USDT',
                  'XAI_USDT']

    db_num = {'orderbooks': 0, 'prices': 1, 'orders': 2, 'trades': 3}
    demo = False

    while True:
        try:
            redis_client = redis.Redis(db=db_num['orderbooks'], decode_responses=True)
            logger.info(f'Redis connected: {redis_client.ping()}')
            redis_client.flushdb()
            logger.info('Очищаем таблицу')

            asyncio.run(main(demo=demo, token_list=token_list))
        except (KeyboardInterrupt, CancelledError):
            logger.info('Завершение работы.')
            break
        except ConnectionLostError as err:
            sleep(10)
            logger.info('Рестарт программы.')
            asyncio.run(main(demo=demo, token_list=token_list))
