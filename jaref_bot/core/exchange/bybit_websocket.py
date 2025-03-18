import orjson
import hmac
import time
import logging

from jaref_bot.core.exchange.base_websocket import BaseWebSocketClient
from jaref_bot.config import credentials as cr

logging.basicConfig(format="%(message)s", level=logging.INFO)

logger = logging.getLogger()

class BybitWebSocketClient(BaseWebSocketClient):
    def __init__(self, demo: bool = False):
        super().__init__(demo)

        self.exchange = 'bybit'
        self.api_key = cr.BYBIT_DEMO_API_KEY if self.demo else cr.BYBIT_API_KEY
        self.api_secret = cr.BYBIT_DEMO_SECRET_KEY if self.demo else cr.BYBIT_SECRET_KEY

    def _create_symbol_name(self, symbol, **kwargs):
        return ''.join(symbol.split('_'))

    def prepare_signature(self, expires: int) -> str:
        return hmac.new(self.api_secret.encode("utf-8"),
                    f"GET/realtime{expires}".encode("utf-8"),
                    digestmod="sha256").hexdigest()

    async def authenticate(self, endpoint: str):
        expires = int(time.time() * 1000) + 1000
        signature = self.prepare_signature(expires)
        auth_msg = {"op": "auth", "args": [self.api_key, expires, signature]}
        await self.connections[endpoint].send(orjson.dumps(auth_msg))
        logger.debug(f"Sent authentication request to Bybit ({endpoint})")

    async def default_handler(self, stream_type, msg):
        event = msg.get('op')
        status = msg.get('success')
        # print(f'{msg=}')

        if event == 'auth' and status:
            logger.info(f'Connected to ByBit')
        elif event == 'subscribe' and status:
            logger.info(f'Subscribed to ByBit.{stream_type}')
            self.connected = True
        elif event == 'subscribe' and not status:
            logger.info(f'Ошибка подключения к стриму ByBit.{stream_type}')
        elif stream_type == 'order':
            await self.handle_order_stream(stream_type, msg)
        elif stream_type == 'ticker':
            await self.handle_ticker_stream(msg)
        elif stream_type == 'orderbook':
            await self.handle_orderbook_stream(msg)
        else:
            print(f'bybit default handler: {msg}')

    async def handle_ticker_stream(self, msg):
        topic = msg.get('topic', '').split('.')[-1]
        self.db_client.hset(topic, mapping=msg.get('data', {}))

    async def handle_orderbook_stream(self, msg):
        msg_type = msg.get('type', '')
        data = msg.get('data', {})
        tkn = data.get('s', '')
        symbol = tkn[:-4] + '_' + tkn[-4:]
        cts = msg.get('cts', 0) // 1000
        bid = data.get('b', {})
        ask = data.get('a', {})

        bid_key = f"orderbook:bybit:linear:{symbol}:bids"
        ask_key = f"orderbook:bybit:linear:{symbol}:asks"
        time_key = f"orderbook:bybit:linear:{symbol}:update_time"

        pipe = self.db_client.pipeline()

        if msg_type == "snapshot":
            pipe.delete(bid_key, ask_key)
            for price_str, amount_str in bid:
                pipe.hset(bid_key, price_str, amount_str)
            for price_str, amount_str in ask:
                pipe.hset(ask_key, price_str, amount_str)

        elif msg_type == "delta":
            if bid:
                for price_str, amount_str in bid:
                    if float(amount_str) == 0:
                        pipe.hdel(bid_key, price_str)
                    else:
                        pipe.hset(bid_key, price_str, amount_str)
            if ask:
                for price_str, amount_str in ask:
                    if float(amount_str) == 0:
                        pipe.hdel(ask_key, price_str)
                    else:
                        pipe.hset(ask_key, price_str, amount_str)

        pipe.hset(time_key, "cts", cts)
        pipe.execute();

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

    async def subscribe_position_stream(self):
        sub_msg = {"op": "subscribe", "args": ['position']}
        await self.subscribe(endpoint='private', channel='position', sub_msg=sub_msg)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": ['order']}
        await self.subscribe(endpoint='private', channel='order', sub_msg=sub_msg)

    async def subscribe_ticker_stream(self, ticker):
        sub_msg = {"op": "subscribe", "args": [f'tickers.{ticker}']}
        await self.subscribe(endpoint='linear', channel='ticker', sub_msg=sub_msg)

    async def subscribe_orderbook_stream(self, depth, tickers):
        bybit_tokens = list(self.coin_info['bybit_linear'].keys())
        token_list = [tok for tok in tickers if tok in bybit_tokens]
        logger.info(f'{len(token_list)} Bybit connections.')
        args = []

        for token in token_list:
            sym = self._create_symbol_name(token)
            args.append(f'orderbook.{depth}.{sym}')

        sub_msg = {"op": "subscribe", "args": args}
        # print(sub_msg)
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)
