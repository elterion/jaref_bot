import asyncio
import json
import hmac
import time
import websockets
from typing import Dict, List, Callable, Any

from config import BYBIT_DEMO_API_KEY, BYBIT_DEMO_SECRET_KEY
from jaref_bot.db.db_manager import DBManager
from config import host, user, password, db_name

from psycopg2.errors import UniqueViolation

db_params = {'host': host, 'user': user, 'password': password, 'database': db_name}
db_manager = DBManager(db_params)

def bybit_signature(api_secret, expires):
    return hmac.new(api_secret.encode("utf-8"),
                f"GET/realtime{expires}".encode("utf-8"),
                digestmod="sha256").hexdigest()

class WebSocketClient:
    def __init__(self, exchange: str, urls: Dict[str, str], api_key: str = None, api_secret: str = None):
        self.exchange = exchange
        self.urls = urls  # Словарь, содержащий URL для различных типов подписок
        self.api_key = api_key
        self.api_secret = api_secret
        self.connections = {}  # Хранение активных соединений
        self.subscriptions = {}
        self.lock = asyncio.Lock()
        self.handlers = {}

    def register_handler(self, stream_type: str, handler: Callable[[Dict[str, Any]], None]):
        self.handlers[stream_type] = handler

    async def connect(self, stream_type: str):
        url = self.urls[stream_type]
        async with websockets.connect(url, ping_interval=10) as ws:
            self.connections[stream_type] = ws
            print(f'Connected to {self.exchange} ({stream_type})')
            if self.api_key and self.api_secret:
                await self.authenticate(stream_type)
            await self.resubscribe(stream_type)
            await self.listen(ws, stream_type)

    async def authenticate(self, stream_type: str):
        if self.exchange == "Bybit":
            await self.authenticate_bybit(stream_type)
        elif self.exchange == "OKX":
            await self.authenticate_okx(stream_type)
        elif self.exchange == "Gate.io":
            await self.authenticate_gateio(stream_type)

    async def authenticate_bybit(self, stream_type: str):
        expires = int(time.time() * 1000) + 1000
        signature = bybit_signature(self.api_secret, expires)
        auth_msg = {"op": "auth", "args": [self.api_key, expires, signature]}
        await self.connections[stream_type].send(json.dumps(auth_msg))
        print(f"Sent authentication request to Bybit ({stream_type})")

    async def authenticate_okx(self, stream_type: str):
        print(f"OKX authentication not implemented yet ({stream_type})")

    async def authenticate_gateio(self, stream_type: str):
        print(f"Gate.io authentication not implemented yet ({stream_type})")

    async def resubscribe(self, stream_type: str):
        for sub in self.subscriptions.get(stream_type, set()):
            await self.subscribe(stream_type, sub)

    async def subscribe(self, stream_type: str, channel: str):
        async with self.lock:
            if stream_type not in self.subscriptions:
                self.subscriptions[stream_type] = set()
            if channel not in self.subscriptions[stream_type]:
                self.subscriptions[stream_type].add(channel)
                sub_msg = {"op": "subscribe", "args": [channel]}

                await self.connections[stream_type].send(json.dumps(sub_msg))
                print(f"Subscribed to {channel} on {self.exchange} ({stream_type})")

    async def listen(self, ws, stream_type: str):
        async for msg in ws:
            try:
                data = json.loads(msg)

                if 'success' in data:
                    handler = self.default_handler
                else:
                    handler = self.handlers.get(stream_type, self.default_handler)
                await handler(stream_type, data)

            except json.JSONDecodeError:
                print(f"Failed to parse message: {msg}")

    async def default_handler(self, stream_type, msg: Dict[str, Any]):
        if msg.get('op') in ('auth', 'subscribe'):
            pass
        else:
            print(f'default handler: {msg}')

async def handle_bybit_private(stream_type, msg: Dict[str, Any]):
    if 'data' in msg:
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
            print(f"Open order. {side} {qty} {symbol} for {usdt_value} with fee {usdt_fee}")
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
        except UniqueViolation:
            print(f"Close order. {side} {qty} {symbol} for {usdt_value} with fee {usdt_fee}")
            db_manager.close_order(token=symbol, exchange='bybit', market_type=market_type,
                close_price=price, close_avg_price=avg_price, close_usdt_amount=usdt_value,
                qty=qty, close_fee=usdt_fee)
    else:
        print(f"[Private ERROR!]: {msg}")

async def main(demo=False):
    if demo:
        bybit_private_url = "wss://stream-demo.bybit.com/v5/private"
    else:
        bybit_private_url = "wss://stream.bybit.com/v5/private"

    bybit_client = WebSocketClient(
        exchange="Bybit",
        urls = {
            "private": bybit_private_url,
        },
        api_key = BYBIT_DEMO_API_KEY,
        api_secret = BYBIT_DEMO_SECRET_KEY
    )
    bybit_client.register_handler("private", handle_bybit_private)


    # okx_client = WebSocketClient(
    #     exchange="OKX",
    #     urls={
    #         "public": "wss://ws.okx.com:8443/ws/v5/public",
    #         "private": "wss://ws.okx.com:8443/ws/v5/private"
    #     }
    # )


    async def manage_subscriptions():
        await asyncio.sleep(2)  # Подождем, пока установится соединение
        # await bybit_client.subscribe("private", "position")
        await bybit_client.subscribe("private", "order")
        # print('Active subscriptions:', bybit_client.subscriptions)

    await asyncio.gather(
        bybit_client.connect("private"),
        # okx_client.connect("private"),
        manage_subscriptions()
    )

if __name__ == "__main__":
    asyncio.run(main(demo=True))
