import asyncio
from asyncio.exceptions import CancelledError
import logging
import redis
from jaref_bot.core.exchange.bybit_websocket import BybitWebSocketClient
from jaref_bot.core.exchange.okx_websocket import OkxWebSocketClient
from jaref_bot.core.exchange.gate_websocket import GateWebSocketClient

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


async def manage_subscriptions(bybit_client=None, okx_client=None, gate_client=None, token_list=None):
    await bybit_client.subscribe_orderbook_stream(depth=50, tickers=token_list)
    await okx_client.subscribe_orderbook_stream(depth=5, tickers=token_list)
    await gate_client.subscribe_orderbook_stream(depth=5, tickers=token_list)

async def main(demo=False, token_list=None):
    bybit_client = BybitWebSocketClient(demo=demo)
    okx_client = OkxWebSocketClient(demo=demo)
    gate_client = GateWebSocketClient(demo=demo)

    tasks = [
        bybit_client.connect("orderbook", auth_required=False),
        okx_client.connect("orderbook", auth_required=False),
        gate_client.connect("orderbook", auth_required=False),

        manage_subscriptions(bybit_client=bybit_client,
                             okx_client=okx_client,
                             gate_client=gate_client,
                             token_list=token_list)
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    token_list = ['ATA_USDT', 'BROCCOLI_USDT', 'FUEL_USDT', 'SERAPH_USDT']

    try:
        db_client = redis.Redis(db=0, decode_responses=True)
        print('Redis connected:', db_client.ping())
        db_client.flushdb()
        print('Очищаем таблицу')

        asyncio.run(main(demo=False, token_list=token_list))
    except (KeyboardInterrupt, CancelledError):
        print('Завершение работы.')
