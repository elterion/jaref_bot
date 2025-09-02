import json
import hmac
import time
import logging
import base64
import pickle
import redis

from jaref_bot.core.exchange.base_websocket import BaseWebSocketClient
from jaref_bot.config import credentials as cr

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

with open("./data/coin_information.pkl", "rb") as f:
    coin_information = pickle.load(f)

market_fees = {'okx_spot': 0.001, 'okx_linear': 0.0005}

class OkxWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo = False):
        super().__init__(demo)

        self.exchange = 'okx'
        self.api_key = cr.OKX_DEMO_API_KEY if self.demo else cr.OKX_API_KEY
        self.api_secret = cr.OKX_DEMO_SECRET_KEY if self.demo else cr.OKX_SECRET_KEY
        self.api_passhrase = cr.OKX_DEMO_PASSPHRASE if self.demo else cr.OKX_PASSPHRASE

        self.subscription_confirmation = False

    def _create_symbol_name(self, market_type, symbol):
        sym = '-'.join(symbol.split('_'))
        if market_type == 'linear':
            sym += '-SWAP'
        return sym

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
            logger.info(f'Connected to OKX')
        elif event in ('channel-conn-count', 'channel-conn-count-error'):
            logger.info(f'OKX n_connections: {msg.get('connCount', 0)}')
        elif event == 'subscribe' and msg.get('connId'):
            self.connected = True
            if not self.subscription_confirmation:
                logger.info(f'Subscribed to OKX.{stream_type}')
                self.subscription_confirmation = True
        elif event == 'subscribe': # В случае, если при подписке произошла ошибка
            print(msg)
        elif channel == 'orders':
            await self.handle_order_stream(stream_type, msg)
        elif stream_type == 'orderbook':
            await self.handle_orderbook_stream(msg)
        elif stream_type == 'public_trades':
            await self.handle_public_trades_stream(msg)
        else:
            print(f'okx default handler: {msg}')

    async def handle_order_stream(self, stream_type, msg):
        redis_client = redis.Redis(db=self.DB_NUM['orders'], decode_responses=True)
        data = msg['data'][0]

        order_id = data['ordId']
        status = data['state']
        symbol = data['instId'].split('-')[0] + '_USDT'
        qty = float(data['sz'])

        market_type = data['instType'].lower()
        if market_type == 'swap':
            market_type = 'linear'
            ct_val = float(coin_information['okx_linear'][symbol]['ct_val'])
            qty *= ct_val

        side = data['side'].lower()

        if status == 'live':
            price = float(data['px'])
            print(f'[ORDER PLACED] {side} {qty} {symbol} at {price}')
            redis_client.hset(name=f'pending_orders:bybit:{symbol}',
                              mapping={'qty': qty, 'side': side})
        elif status == 'canceled':
            price = float(data['px'])
            print(f'[ORDER CANCELLED] {side} {qty} {symbol} at {price}')
            redis_client.delete(f'pending_orders:bybit:{symbol}')
        elif status == 'filled':
            price = float(data['fillPx'])
            usdt_fee = abs(float(data['fillFee']))
            print(f'[ORDER FILLED] {side} {qty} {symbol} for {usdt_value}')
        else:
            print(data)

            price = float(data['avgPx'] or data['px'] or 0)
            usdt_value = float(data['fillNotionalUsd'] or data['notionalUsd'] or 0)

            usdt_fee = abs(float(data['fillFee'] or data['fee'] or 0))
            if usdt_fee < 0.00001:
                fee = float(market_fees[f'okx_{market_type}']) * usdt_value

    async def handle_orderbook_stream(self, msg):
        data = msg.get('data', [])
        if data:
            data = data[0]

        toks = msg.get('arg', '').get('instId', '').split('-')
        token = toks[0] + '_' + toks[1]
        market_type = 'linear' if (len(toks) == 3 and toks[2] == 'SWAP') else 'spot'

        ct_val = float(coin_information[f'okx_{market_type}'][token]['ct_val'])
        bids = [(price, float(vol) * ct_val) for price, vol, v1, v2 in data['bids']]
        asks = [(price, float(vol) * ct_val) for price, vol, v1, v2 in data['asks']]
        ts = int(data['ts']) // 1000

        bid_key = f"orderbook:okx:{market_type}:{token}:bids"
        ask_key = f"orderbook:okx:{market_type}:{token}:asks"
        time_key = f"orderbook:okx:{market_type}:{token}:update_time"

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

    async def handle_public_trades_stream(self, msg):
        exc = 'okx'
        data = msg.get('data', [])

        for trade in data:
            ts = trade.get('ts', None)
            sym_str = trade.get('instId', None)
            if sym_str:
                syms = sym_str.split('-')
                symbol = syms[0] + '_' + syms[1]
            else:
                symbol = None

            market_type = 'linear' if sym_str.endswith('SWAP') else 'spot'
            ct_val = float(coin_information[f'okx_{market_type}'][symbol]['ct_val'])
            price_scale = coin_information[f'okx_{market_type}'][symbol]['price_scale']

            side = trade.get('side', '')
            volume = round(float(trade.get('sz', None)) * ct_val, price_scale)
            price = trade.get('px', 0)

            print(f'{exc=}; {symbol=}; {side=}; {volume=}; {price=}')
            self.trades_client.add_trade_to_stream(exc=exc, ts=ts, symbol=symbol, side=side, price=price, volume=volume)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": [
            {"channel": "orders", "instType": "SPOT"},
            {"channel": "orders", "instType": "SWAP"}
            ]}
        await self.subscribe(endpoint='private', sub_msg=sub_msg)

    async def subscribe_orderbook_stream(self, depth, tickers):
        okx_tokens = list(self.coin_info['okx_linear'].keys())
        token_list = [tok for tok in tickers if tok in okx_tokens]
        logger.info(f'{len(token_list)} Okx connections.')

        if len(token_list) == 0:
            return

        args = []
        for token in token_list:
            sym = self._create_symbol_name(market_type='linear', symbol=token)
            args.append(dict(channel='books5', instId=sym))

        sub_msg = {"op": "subscribe", "args": args}
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)

    async def subscribe_public_trades_stream(self, tickers):
        okx_tokens = list(self.coin_info['okx_linear'].keys())
        token_list = [tok for tok in tickers if tok in okx_tokens]
        logger.info(f'{len(token_list)} Okx connections.')

        if len(token_list) == 0:
            return

        args = []
        for token in token_list:
            sym = self._create_symbol_name(market_type='linear', symbol=token)
            args.append(dict(channel='trades', instId=sym))

        sub_msg = {"op": "subscribe", "args": args}
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)
