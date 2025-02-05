import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from abc import ABC, abstractmethod

import asyncio
import json
import hmac
import base64
import time
import websockets
from websockets.exceptions import ConnectionClosedError
from typing import Dict, List, Callable, Any
import logging

from config import BYBIT_DEMO_API_KEY, BYBIT_DEMO_SECRET_KEY, BYBIT_API_KEY, BYBIT_SECRET_KEY
from config import OKX_DEMO_API_KEY, OKX_DEMO_SECRET_KEY, OKX_API_KEY, OKX_SECRET_KEY, OKX_DEMO_PASSPHRASE, OKX_PASSPHRASE
from jaref_bot.db.db_manager import DBManager
from config import host, user, password, db_name

from psycopg2.errors import UniqueViolation

db_params = {'host': host, 'user': user, 'password': password, 'database': db_name}
db_manager = DBManager(db_params)

logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger()

class BaseWebSocketClient(ABC):
    def __init__(self, urls: Dict[str, str], demo: bool = False):
        self.exchange = None
        self.urls = urls  # Словарь, содержащий URL для различных типов подписок
        self.demo = demo
        self.api_key = None
        self.api_secret = None
        self.api_passhrase = None
        self.connections = {}  # Хранение активных соединений
        self.subscriptions = {}
        self.lock = asyncio.Lock()
        self.handlers = {}
        self.is_connected = False

    @abstractmethod
    async def authenticate(self, stream_type: str):
        pass

    async def connect(self, stream_type: str):
        url = self.urls[stream_type]

        async for websocket in websockets.connect(url, ping_interval=10):
            try:
                self.connections[stream_type] = websocket

                logger.debug(f'Connecting to {self.exchange} ({stream_type})')
                if url.endswith('private'):
                    await self.authenticate(stream_type)
                await self.resubscribe(stream_type)
                await self.listen(websocket, stream_type)
            except ConnectionClosedError as err:
                logger.error(f'{self.exchange} connection closed! {err}')
                self.is_connected = False
                continue

            await asyncio.sleep(2)
            await self.connect(stream_type)

    async def resubscribe(self, stream_type: str):
        for stream_type, data in self.subscriptions.items():
            logger.debug(f'{stream_type=}, {data=}')
            channel = data['channel']
            sub_msg = data['sub_msg']
            await self._subscribe(stream_type, channel, sub_msg)

    async def _subscribe(self, stream_type: str, channel: str, sub_msg):
        async with self.lock:
            while not self.is_connected:
                await asyncio.sleep(0.1)

            if stream_type not in self.subscriptions:
                self.subscriptions[stream_type] = dict()
            if channel not in self.subscriptions[stream_type]:
                self.subscriptions[stream_type].update({'channel': channel, 'sub_msg': sub_msg})
                logger.debug(f'{sub_msg=}')

                await self.connections[stream_type].send(json.dumps(sub_msg))
                logger.debug(f"Subscribing to {channel} on {self.exchange} ({stream_type})")

    async def listen(self, ws, stream_type: str):
        async for msg in ws:
            try:
                data = json.loads(msg)
                await self.response_formatting(stream_type, data)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse message: {msg}")

    @abstractmethod
    async def response_formatting(self, stream_type, data):
        pass

    @abstractmethod
    async def default_handler(self, stream_type, msg):
        pass


class BybitWebSocketClient(BaseWebSocketClient):
    def __init__(self, urls, demo = False):
        super().__init__(urls, demo)

        self.exchange = 'bybit'
        self.api_key = BYBIT_DEMO_API_KEY if self.demo else BYBIT_API_KEY
        self.api_secret = BYBIT_DEMO_SECRET_KEY if self.demo else BYBIT_SECRET_KEY

    def prepare_signature(self, expires):
        return hmac.new(self.api_secret.encode("utf-8"),
                    f"GET/realtime{expires}".encode("utf-8"),
                    digestmod="sha256").hexdigest()

    async def authenticate(self, stream_type: str):
        expires = int(time.time() * 1000) + 1000
        signature = self.prepare_signature(expires)
        auth_msg = {"op": "auth", "args": [self.api_key, expires, signature]}
        await self.connections[stream_type].send(json.dumps(auth_msg))
        logger.debug(f"Sent authentication request to Bybit ({stream_type})")

    async def response_formatting(self, stream_type, data):
        if 'success' in data:
            handler = self.default_handler
        else:
            handler = self.handlers.get(stream_type, self.default_handler)
        await handler(stream_type, data)

    async def default_handler(self, stream_type, msg):
        event = msg.get('op')
        status = msg.get('success')

        if event == 'auth' and status:
            self.is_connected = True
            logger.info(f'Connected to {self.exchange}')
        elif event == 'subscribe' and status:
            logger.info(f'Subscribed to {self.exchange}.{stream_type}')
        elif msg.get('data'):
            await self.handle_order_stream(stream_type, msg)
        else:
            print(f'bybit default handler: {msg}')

    async def handle_order_stream(self, stream_type, msg):
        data = msg['data'][0]

        order_id = data['orderId']
        symbol = data['symbol']

        market_type = data['category'].lower()
        order_status = data['orderStatus']
        side = data['side'].lower()
        price = data['price']
        avg_price = data['avgPrice']
        qty = data['qty']
        usdt_value = data['cumExecValue']
        usdt_fee = data['cumExecFee']
        order_type = data['orderType'].lower()

        trig_price = data['triggerPrice']
        trig_dir = data['triggerDirection']
        tp = data['takeProfit']
        sl = data['stopLoss']

        try:
            db_manager.add_order(token=symbol,
                                exchange='bybit',
                                market_type=market_type,
                                order_type=order_type,
                                order_id=order_id,
                                order_side=side,
                                price=price,
                                avg_price=avg_price,
                                usdt_amount=usdt_value,
                                qty=qty,
                                fee=usdt_fee,
                                )
            print(f"Open order. {side} {qty} {symbol} for {usdt_value} with fee {usdt_fee}")
        except UniqueViolation:
            db_manager.close_order(token=symbol, exchange='bybit', market_type=market_type,
                close_price=price, close_avg_price=avg_price, close_usdt_amount=usdt_value,
                qty=qty, close_fee=usdt_fee)
            print(f"Close order. {side} {qty} {symbol} for {usdt_value} with fee {usdt_fee}")

    async def subscribe_position_stream(self):
        sub_msg = {"op": "subscribe", "args": ['position']}
        await self._subscribe(stream_type='private', channel='position', sub_msg=sub_msg)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": ['order']}
        await self._subscribe(stream_type='private', channel='order', sub_msg=sub_msg)

class OkxWebSocketClient(BaseWebSocketClient):
    def __init__(self, urls, demo = False):
        super().__init__(urls, demo)

        self.exchange = 'okx'
        self.api_key = OKX_DEMO_API_KEY if self.demo else OKX_API_KEY
        self.api_secret = OKX_DEMO_SECRET_KEY if self.demo else OKX_SECRET_KEY
        self.api_passhrase = OKX_DEMO_PASSPHRASE if self.demo else OKX_PASSPHRASE
        logger.debug(f'{self.demo=}, {self.urls=}')

    def prepare_signature(self, ts):
        sign = ts + 'GET' + '/users/self/verify'
        mac = hmac.new(bytes(self.api_secret, encoding='utf-8'),
                       bytes(sign, encoding='utf-8'),
                       digestmod='sha256')
        return base64.b64encode(mac.digest()).decode(encoding='utf-8')

    async def authenticate(self, stream_type: str):
        ts = str(int(time.time()))
        signature = self.prepare_signature(ts)
        args = {'apiKey': self.api_key,
                'passphrase': self.api_passhrase,
                'timestamp': ts,
                'sign': signature}
        auth_msg = {"op": "login", "args": [args]}
        await self.connections[stream_type].send(json.dumps(auth_msg))
        logger.debug(f"Sent authentication request to Okx ({stream_type})")

    async def response_formatting(self, stream_type, data):
        if 'success' in data:
            handler = self.default_handler
        else:
            handler = self.handlers.get(stream_type, self.default_handler)
        await handler(stream_type, data)

    async def default_handler(self, stream_type, msg):
        event = msg.get('event')
        status = msg.get('code')

        if event == 'login' and status == '0':
            self.is_connected = True
            logger.info(f'Connected to {self.exchange}')
        elif event == 'subscribe' and msg.get('connId'):
            logger.info(f'Subscribed to {self.exchange}.{stream_type}')
        else:
            print(f'okx default handler: {msg}')

    # async def subscribe_mark_price_stream(self):
    #     sub_msg = {"op": "subscribe", "args": [
    #         {"channel": "mark-price", "instId": "BTC-USDT"},
    #     ]}
    #     await self._subscribe(stream_type='public', channel='mark-price', sub_msg=sub_msg)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": [
            {"channel": "orders", "instType": "SPOT"},
            {"channel": "orders", "instType": "SWAP"}
            ]}
        await self._subscribe(stream_type='private', channel='order', sub_msg=sub_msg)





async def main(demo=False):
    if demo:
        bybit_private_url = "wss://stream-demo.bybit.com/v5/private"
        okx_private_url = "wss://wspap.okx.com:8443/ws/v5/private"
    else:
        bybit_private_url = "wss://stream.bybit.com/v5/private"
        okx_private_url = "wss://ws.okx.com:8443/ws/v5/private"

    bybit_client = BybitWebSocketClient(urls = {"private": bybit_private_url},
        demo=True)

    okx_client = OkxWebSocketClient(
        urls={"private": okx_private_url}, demo=True)


    async def manage_subscriptions():
        # await asyncio.sleep(5)  # Подождем, пока установится соединение
        # await bybit_client.subscribe("private", "position")
        await bybit_client.subscribe_order_stream()
        await okx_client.subscribe_order_stream()
        # logger.info('Active subscriptions:', bybit_client.subscriptions)

    await asyncio.gather(
        bybit_client.connect("private"),
        okx_client.connect("private"),
        # okx_client.connect("private"),
        manage_subscriptions()
    )

if __name__ == "__main__":
    asyncio.run(main(demo=True))
