import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from abc import ABC, abstractmethod

import asyncio
import json
import hmac
import hashlib
import base64
import time
import websockets
import socket
from websockets.exceptions import ConnectionClosedError, ConnectionClosed
from typing import Dict, List, Callable, Any
import logging
import pickle
from decimal import Decimal

from config import BYBIT_DEMO_API_KEY, BYBIT_DEMO_SECRET_KEY, BYBIT_API_KEY, BYBIT_SECRET_KEY
from config import OKX_DEMO_API_KEY, OKX_DEMO_SECRET_KEY, OKX_API_KEY, OKX_SECRET_KEY, OKX_DEMO_PASSPHRASE, OKX_PASSPHRASE
from config import GATE_DEMO_API_KEY, GATE_DEMO_SECRET_KEY
from jaref_bot.db.db_manager import DBManager
from config import host, user, password, db_name

from psycopg2.errors import UniqueViolation
from jaref_bot.core.exceptions.database import OrderNotFoundError

def get_order_status(exchange, market_type, token):
    order_in_pending = db_manager.order_exists(table_name='pending_orders', exchange=exchange, market_type=market_type, token=token)
    order_in_current = db_manager.order_exists(table_name='current_orders', exchange=exchange, market_type=market_type, token=token)

    if not (order_in_pending or order_in_current):
        status = None
    elif order_in_pending and order_in_current:
        status = 'adding'
    elif order_in_pending and not order_in_current:
        status = 'placed'
    elif not order_in_pending and order_in_current:
        status = 'live'
    return status

db_params = {'host': host, 'user': user, 'password': password, 'database': db_name}
db_manager = DBManager(db_params)

logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger()

market_fees = {'bybit_spot': 0.0018, 'bybit_linear': 0.001,
               'okx_spot': 0.001, 'okx_linear': 0.0005,
               'gate_spot': 0.002, 'gate_linear': 0.0005}

with open("./data/coin_information.pkl", "rb") as f:
    coin_information = pickle.load(f)

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

    async def connect(self, stream_type: str, auth_required=False):
        url = self.urls[stream_type]
        reconnect_delay = 1  # Начальная задержка


        while True:  # Бесконечный цикл переподключения
            try:
                logger.debug(f'Connecting to {self.exchange} ({stream_type}): {url}')
                async with websockets.connect(url, ping_interval=15, ping_timeout=40) as websocket:
                    self.connections[stream_type] = websocket
                    logger.info(f'Connected to {self.exchange}')
                    reconnect_delay = 1  # Сброс задержки после успешного подключения

                    if auth_required:
                        logger.debug(f'Sending auth request')
                        await self.authenticate(stream_type)

                    await self.resubscribe(stream_type)
                    logger.debug(f'Listening...')
                    await self.listen(websocket, stream_type)
            except (ConnectionClosedError, ConnectionClosed) as err:
                logger.error(f'{self.exchange} connection closed! {err}')
                self.is_connected = False
            except socket.gaierror as err:
                print(f"DNS resolution failed for {url}: {err}")
                self.is_connected = False
                await asyncio.sleep(reconnect_delay)
            except Exception as err:
                print(f"Unexpected error in {stream_type}: {err}")

            # Экспоненциальный backoff с верхним ограничением в 60 сек
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # Удваиваем до 60 сек


    async def resubscribe(self, stream_type: str):
        logger.debug(f'{self.subscriptions=}')
        for stream_type, data in self.subscriptions.items():
            logger.debug(f'{stream_type=}, {data=}')
            channel = data['channel']
            sub_msg = data['sub_msg']
            await self._subscribe(stream_type, channel, sub_msg)

    async def _subscribe(self, stream_type: str, channel: str, sub_msg):
        async with self.lock:
            await asyncio.sleep(2)

            if stream_type not in self.subscriptions:
                self.subscriptions[stream_type] = dict()
                logger.debug(f'Adding new subscription: {stream_type}')
            if channel not in self.subscriptions[stream_type]:
                self.subscriptions[stream_type].update({'channel': channel, 'sub_msg': sub_msg})
                logger.debug(f'All subscriptions: {self.subscriptions}')
                logger.debug(f'{sub_msg=}')

                await self.connections[stream_type].send(json.dumps(sub_msg))
                logger.debug(f"Subscribing to {channel} on {self.exchange} ({stream_type})")

    async def listen(self, ws, stream_type: str):
        async for msg in ws:
            try:
                data = json.loads(msg)
                await self.default_handler(stream_type, data)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse message: {msg}")

    async def default_handler(self, stream_type, msg):
        print(f'default handler. {stream_type=}, {msg=}')


class BybitWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo = False):
        if demo:
            urls = {"private": "wss://stream-demo.bybit.com/v5/private"}
        else:
            urls = {"private": "wss://stream.bybit.com/v5/private"}

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

    async def default_handler(self, stream_type, msg):
        event = msg.get('op')
        status = msg.get('success')

        if event == 'auth' and status:
            pass
        elif event == 'subscribe' and status:
            logger.info(f'Subscribed to {self.exchange}.{stream_type}')
        elif msg.get('data'):
            await self.handle_order_stream(stream_type, msg)
        else:
            print(f'bybit default handler: {msg}')

    async def handle_order_stream(self, stream_type, msg):
        data = msg['data'][0]

        order_id = data['orderId']
        symbol = data['symbol'][:-4] + '_USDT'

        market_type = data['category'].lower()
        side = data['side'].lower()
        qty = data['cumExecQty']
        price = float(data['avgPrice'])
        usdt_value = float(data['cumExecValue'])
        usdt_fee = abs(float(data['cumExecFee']))

        status = get_order_status(exchange='bybit', market_type=market_type, token=symbol)

        if status in ('placed', 'adding'):
            db_manager.fill_order(token=symbol,
                                exchange='bybit',
                                market_type=market_type,
                                order_id=order_id,
                                qty=qty,
                                price=price,
                                usdt_amount=usdt_value,
                                fee=usdt_fee,
                                )
            print(f"[OPEN ORDER] Bybit. {side} {qty} {symbol} for {usdt_value:.2f}")
        elif status == 'live':
            db_manager.close_order(token=symbol,
                                exchange='bybit',
                                market_type=market_type,
                                close_price=price,
                                close_usdt_amount=usdt_value,
                                close_fee=usdt_fee,
                                closed_at=None)
            print(f"[CLOSE ORDER] Bybit. {side} {qty} {symbol} for {usdt_value:.2f}")
        else:
            raise Exception('Неизвестное состояние ордера на бирже Bybit')

    async def subscribe_position_stream(self):
        sub_msg = {"op": "subscribe", "args": ['position']}
        await self._subscribe(stream_type='private', channel='position', sub_msg=sub_msg)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": ['order']}
        await self._subscribe(stream_type='private', channel='order', sub_msg=sub_msg)

class OkxWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo = False):
        if demo:
            urls = {"private": "wss://wspap.okx.com:8443/ws/v5/private"}
        else:
            urls = {"private": "wss://ws.okx.com:8443/ws/v5/private"}
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

    async def default_handler(self, stream_type, msg):
        event = msg.get('event')
        status = msg.get('code')
        channel = msg.get('arg', {}).get('channel')

        if event == 'login' and status == '0':
            self.is_connected = True
        elif event == 'channel-conn-count':
            pass
        elif event == 'subscribe' and msg.get('connId'):
            logger.info(f'Subscribed to {self.exchange}.{stream_type}')
        elif channel == 'orders' and msg['data'][0]['state'] in ('filled', 'partially_filled'):
            await self.handle_order_stream(stream_type, msg)
        elif channel == 'orders' and msg['data'][0]['state'] == 'live':
            pass
        else:
            print(f'okx default handler: {msg}')

    async def handle_order_stream(self, stream_type, msg):
        data = msg['data'][0]

        order_id = data['ordId']
        symbol = data['instId'].split('-')[0] + '_USDT'
        qty = float(data['sz'])

        market_type = data['instType'].lower()
        if market_type == 'swap':
            market_type = 'linear'
            ct_val = float(coin_information['okx_linear'][symbol]['ct_val'])
            qty *= ct_val

        side = data['side'].lower()
        usdt_fee = abs(float(data['fillFee'] or data['fee'] or 0))


        price = float(data['avgPx'] or data['px'] or 0)
        usdt_value = float(data['fillNotionalUsd'] or data['notionalUsd'] or 0)

        if usdt_fee < 0.00001:
            fee = float(market_fees[f'gate_{market_type}']) * usdt_value

        status = get_order_status(exchange='okx', market_type=market_type, token=symbol)

        if status in ('placed', 'adding'):
            db_manager.fill_order(token=symbol,
                                exchange='okx',
                                market_type=market_type,
                                order_id=order_id,
                                qty=qty,
                                price=price,
                                usdt_amount=usdt_value,
                                fee=usdt_fee,
                                )
            print(f"[OPEN ORDER] Okx. {side} {qty} {symbol} for {usdt_value:.2f}")
        elif status == 'live':
            db_manager.close_order(token=symbol,
                                exchange='okx',
                                market_type=market_type,
                                close_price=price,
                                close_usdt_amount=usdt_value,
                                close_fee=usdt_fee,
                                closed_at=None)
            print(f"[CLOSE ORDER] Okx. {side} {qty} {symbol} for {usdt_value:.2f}")
        else:
            raise Exception('Неизвестное состояние ордера на бирже Okx')

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": [
            {"channel": "orders", "instType": "SPOT"},
            {"channel": "orders", "instType": "SWAP"}
            ]}
        await self._subscribe(stream_type='private', channel='order', sub_msg=sub_msg)

class GateWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo = False):
        if demo:
            urls = {"gate_demo_futures": "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt"}
        else:
            urls = {"gate_futures": "wss://fx-ws.gateio.ws/v4/ws/usdt"}
        super().__init__(urls, demo)

        self.exchange = 'gate'
        self.api_key = GATE_DEMO_API_KEY if self.demo else None
        self.api_secret = GATE_DEMO_SECRET_KEY if self.demo else None
        self.uid = 20614239
        logger.debug(f'{self.demo=}, {self.urls=}')

    async def authenticate(self, stream_type: str):
        pass

    async def default_handler(self, stream_type, msg):
        event = msg.get('event')
        channel = msg.get('channel')
        if event == 'subscribe':
            status = msg.get('result', {}).get('status')
            if status == 'success':
                self.is_connected = True
                logger.info(f'Subscribed to gate.{channel}')
        elif event == 'update' and channel == 'futures.orders':
            await self.handle_order_stream(stream_type, msg)

    async def handle_order_stream(self, stream_type, msg):
        data = msg['result'][0]
        order_id = data['id']
        symbol = data['contract']
        ct_val = float(coin_information['gate_linear'][symbol]['ct_val'])

        market_type = 'linear' if msg['channel'] == 'futures.orders' else None
        price = data['fill_price']
        qty = abs(float(data['size'])) * ct_val
        usdt_value = float(price) * qty

        fee = market_fees[f'gate_{market_type}']
        usdt_fee = usdt_value * fee

        status = get_order_status(exchange='gate', market_type=market_type, token=symbol)

        if status in ('placed', 'adding'):
            db_manager.fill_order(token=symbol,
                                exchange='gate',
                                market_type=market_type,
                                order_id=order_id,
                                qty=qty,
                                price=price,
                                usdt_amount=usdt_value,
                                fee=usdt_fee,
                                )
            print(f"[OPEN ORDER] Gate. {qty} {symbol} for {usdt_value:.2f}")
        elif status == 'live':
            db_manager.close_order(token=symbol,
                                exchange='gate',
                                market_type=market_type,
                                close_price=price,
                                close_usdt_amount=usdt_value,
                                close_fee=usdt_fee,
                                closed_at=None)
            print(f"[CLOSE ORDER] Gate. {qty} {symbol} for {usdt_value:.2f}")
        else:
            raise Exception('Неизвестное состояние ордера на бирже Gate')

    def prepare_signature(self, channel, event, ts):
        message = f'channel={channel}&event={event}&time={ts}'
        return hmac.new(self.api_secret.encode("utf8"), message.encode("utf8"),
                hashlib.sha512).hexdigest()

    def prepare_auth_subscription_message(self, channel):
        ts = int(time.time())
        sub_msg = {
            "time": ts,
            "channel": channel,
            "event": "subscribe",
            "payload": [str(self.uid), "!all"],
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": self.prepare_signature(channel, event="subscribe", ts=ts)
                }
        }
        return sub_msg

    async def subscribe_futures_positions_stream(self):
        channel = "futures.positions"
        sub_msg = self.prepare_auth_subscription_message(channel)
        logger.debug(f'Sending subscription params: {json.dumps(sub_msg)}')
        await self._subscribe(stream_type='gate_demo_futures', channel=channel, sub_msg=sub_msg)

    async def subscribe_futures_orders_stream(self):
        channel = "futures.orders"
        sub_msg = self.prepare_auth_subscription_message(channel)
        logger.debug(f'Sending subscription params: {json.dumps(sub_msg)}')
        await self._subscribe(stream_type='gate_demo_futures', channel=channel, sub_msg=sub_msg)

    async def subscribe_futures_usertrades_stream(self):
        channel = "futures.usertrades"
        sub_msg = self.prepare_auth_subscription_message(channel)
        logger.debug(f'Sending subscription params: {json.dumps(sub_msg)}')
        await self._subscribe(stream_type='gate_demo_futures', channel=channel, sub_msg=sub_msg)


async def main(demo=False):
    bybit_client = BybitWebSocketClient(demo=True)
    okx_client = OkxWebSocketClient(demo=True)
    gate_client = GateWebSocketClient(demo=True)

    async def manage_subscriptions():
        await bybit_client.subscribe_order_stream()
        await okx_client.subscribe_order_stream()
        await gate_client.subscribe_futures_orders_stream()
        # logger.debug('Active subscriptions:', bybit_client.subscriptions)

    await asyncio.gather(
        bybit_client.connect("private", auth_required=True),
        okx_client.connect("private", auth_required=True),
        gate_client.connect("gate_demo_futures", auth_required=True),
        manage_subscriptions()
    )

if __name__ == "__main__":
    asyncio.run(main(demo=True))
