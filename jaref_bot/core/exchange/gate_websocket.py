import orjson
import json
import hmac
import time
import logging
import hashlib
import pickle

from jaref_bot.core.exchange.base_websocket import BaseWebSocketClient
from jaref_bot.config import credentials as cr

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

with open("./data/coin_information.pkl", "rb") as f:
    coin_information = pickle.load(f)

market_fees = {'gate_spot': 0.002, 'gate_linear': 0.0005}


class GateWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo = False):
        super().__init__(demo)

        self.exchange = 'gate'
        self.api_key = cr.GATE_DEMO_API_KEY if self.demo else cr.GATE_API_KEY
        self.api_secret = cr.GATE_DEMO_SECRET_KEY if self.demo else cr.GATE_SECRET_KEY
        self.uid = 20614239

        self.subscription_confirmation = False

    async def authenticate(self, stream_type: str):
        pass

    async def default_handler(self, stream_type, msg):
        # print(f'{stream_type=}, {msg=}')
        event = msg.get('event')
        channel = msg.get('channel')
        if event == 'subscribe':
            status = msg.get('result', {}).get('status')
            if status == 'success':
                self.connected = True
                if not self.subscription_confirmation:
                    logger.info(f'Subscribed to gate.{channel}')
                    self.subscription_confirmation = True
            else:
                print(msg)
        elif event == 'update' and channel == 'futures.orders':
            await self.handle_order_stream(stream_type, msg)
        elif stream_type == 'orderbook' and channel == 'futures.order_book':
            await self.handle_legacy_orderbook_order_stream(msg)
        elif stream_type == 'public_trades':
            await self.handle_public_trades_stream(msg)

    async def handle_legacy_orderbook_order_stream(self, msg):
        # print(msg)
        data = msg['result']
        token = data['contract']
        ts = msg['time']
        ct_val = float(coin_information[f'gate_linear'][token]['ct_val'])
        asks = [(row['p'], float(row['s']) * ct_val) for row in data['asks']]
        bids = [(row['p'], float(row['s']) * ct_val) for row in data['bids']]

        bid_key = f"orderbook:gate:linear:{token}:bids"
        ask_key = f"orderbook:gate:linear:{token}:asks"
        time_key = f"orderbook:gate:linear:{token}:update_time"

        pipe = self.ob_client.pipeline()
        pipe.delete(bid_key, ask_key)
        for price_str, amount_str in bids:
            pipe.hset(bid_key, price_str, amount_str)
        for price_str, amount_str in asks:
            pipe.hset(ask_key, price_str, amount_str)

        pipe.hset(time_key, "cts", ts)
        pipe.expire(bid_key, 60)
        pipe.expire(ask_key, 60)
        pipe.expire(time_key, 60)
        pipe.execute()

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

    async def handle_public_trades_stream(self, msg):
        exc = 'gate'
        data = msg.get('result', [])
        market_type = 'linear' if msg.get('channel', '').startswith('futures') else 'spot'

        for trade in data:
            ts = trade.get('time_ms', None)
            symbol = trade.get('contract', None)
            ct_val = float(coin_information[f'gate_{market_type}'][symbol]['ct_val'])
            price_scale = coin_information[f'gate_{market_type}'][symbol]['price_scale']

            side = 'buy' if float(trade.get('side', 0)) > 0 else 'sell'
            volume = round(abs(float(trade.get('size', None))) * ct_val, price_scale)
            price = trade.get('price', 0)

            print(f'{exc=}; {symbol=}; {side=}; {volume=}; {price=}')
            self.trades_client.add_trade_to_stream(exc=exc, ts=ts, symbol=symbol, side=side, price=price, volume=volume)

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

    async def subscribe_orderbook_stream(self, depth, tickers):
        gate_tokens = list(self.coin_info['gate_linear'].keys())
        token_list = [tok for tok in tickers if tok in gate_tokens]
        logger.info(f'{len(token_list)} Gate.io connections.')
        channel = "futures.order_book"

        for token in token_list:
            ts = int(time.time())
            sub_msg = {
                "time": ts,
                "channel": channel,
                "event": "subscribe",
                "payload": [token, "5", "0"],
            }

            await self.subscribe(endpoint='linear', sub_msg=sub_msg)

    async def subscribe_public_trades_stream(self, tickers):
        gate_tokens = list(self.coin_info['gate_linear'].keys())
        token_list = [tok for tok in tickers if tok in gate_tokens]
        logger.info(f'{len(token_list)} Gate.io connections.')
        channel = "futures.trades"

        # for token in token_list:
        ts = int(time.time())
        sub_msg = {
            "time": ts,
            "channel": channel,
            "event": "subscribe",
            "payload": token_list,
        }

        await self.subscribe(endpoint='linear', sub_msg=sub_msg)
