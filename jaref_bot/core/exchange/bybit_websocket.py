import orjson
import hmac
import time
import logging
import redis
from datetime import datetime, timezone

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

    def get_stream_type(self, msg):
        stream_type = ''

        if msg.get('op', '') or msg.get('success', ''):
            stream_type = 'system_message'
        elif msg.get('topic', '').startswith('publicTrade'):
            stream_type = 'public_trades'
        elif msg.get('topic', '').startswith('orderbook'):
            stream_type = 'orderbook'

        return stream_type

    async def default_handler(self, msg):
        stream_type = self.get_stream_type(msg)

        if stream_type == 'system_message' and msg.get('success', ''):
            logger.info('Соединение с биржей ByBit установлено.')
        elif stream_type == 'system_message':
            logger.info(msg)
        elif stream_type == 'orderbook':
            await self.handle_orderbook_stream(msg, verbose=False)
        elif stream_type == 'public_trades':
            await self.handle_trades_stream(msg, verbose=True)
        else:
            print(f'bybit default handler: {msg}')

    async def handle_ticker_stream(self, msg):
        topic = msg.get('topic', '').split('.')[-1]
        self.ob_client.hset(topic, mapping=msg.get('data', {}))

    async def handle_orderbook_stream(self, msg, verbose=False):
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

        pipe = self.ob_client.pipeline()

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
        pipe.expire(bid_key, 60)
        pipe.expire(ask_key, 60)
        pipe.expire(time_key, 60)
        pipe.execute()

        if verbose:
            print(f'[orderbook]: Bybit. {symbol=}')

    async def handle_order_stream(self, stream_type, msg):
        redis_client = redis.Redis(db=self.DB_NUM['orders'], decode_responses=True)

        data = msg['data'][0]

        order_id = data['orderId']
        symbol = data['symbol'][:-4] + '_USDT'
        status = data['orderStatus'].lower()

        market_type = data['category'].lower()
        side = data['side'].lower()

        if status == 'new':
            qty = data['qty']
            price = data['price']
            print(f'[ORDER PLACED] {side} {qty} {symbol} at {price}')
            redis_client.hset(name=f'pending_orders:bybit:{symbol}',
                              mapping={'qty': qty, 'side': side})

        elif status == 'filled':
            qty = data['cumExecQty']

            price = float(data['avgPrice'])
            usdt_value = float(data['cumExecValue'])
            usdt_fee = abs(float(data['cumExecFee']))

            print(f'[ORDER FILLED] {side} {qty} {symbol} for {usdt_value}')
        elif status == 'cancelled':
            qty = data['qty']
            price = data['price']
            print(f'[ORDER CANCELLED] {side} {qty} {symbol} at {price}')
            redis_client.delete(f'pending_orders:bybit:{symbol}')
        else:
            print(data)

    async def handle_trades_stream(self, msg, verbose=False):
        exc = 'bybit'
        data = msg.get('data', [])

        for trade in data:
            ts = trade.get('T', None)
            time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            sym_str = trade.get('s', None)

            if sym_str and sym_str.endswith('USDT'):
                symbol = sym_str[:-4] + '_' + sym_str[-4:]
            else:
                symbol = None

            side = trade.get('S', '').lower()
            volume = float(trade.get('v', 0))
            price = float(trade.get('p', 0))

            self.postgre_client.add_public_trade(time=time,
                                                 exchange='bybit',
                                                 symbol=symbol,
                                                 side=side,
                                                 price=price,
                                                 volume=volume)

            if verbose:
                print(f'[public trades]: {time.strftime('%Y-%m-%d %H:%M:%S')}; bybit; {symbol}; {side}; {volume=}; {price=}')

            # self.trades_client.add_trade_to_stream(exc=exc,
            #                                        ts=ts,
            #                                        symbol=symbol,
            #                                        side=side,
            #                                        price=price,
            #                                        volume=volume)


    async def subscribe_position_stream(self):
        sub_msg = {"op": "subscribe", "args": ['position']}
        await self.subscribe(endpoint='private', sub_msg=sub_msg)

    async def subscribe_order_stream(self):
        sub_msg = {"op": "subscribe", "args": ['order']}
        await self.subscribe(endpoint='private', sub_msg=sub_msg)

    async def subscribe_ticker_stream(self, ticker):
        sub_msg = {"op": "subscribe", "args": [f'tickers.{ticker}']}
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)

    async def subscribe_orderbook_stream(self, depth, tickers):
        bybit_tokens = list(self.coin_info['bybit_linear'].keys())
        token_list = [tok for tok in tickers if tok in bybit_tokens]
        logger.info(f'{len(token_list)} Bybit connections.')
        args = []

        for token in token_list:
            sym = self._create_symbol_name(token)
            args.append(f'orderbook.{depth}.{sym}')

        sub_msg = {"op": "subscribe", "args": args}
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)

    async def subscribe_public_trades_stream(self, tickers):
        bybit_tokens = list(self.coin_info['bybit_linear'].keys())
        token_list = [tok for tok in tickers if tok in bybit_tokens]
        logger.info(f'{len(token_list)} Bybit connections.')
        args = []

        for token in token_list:
            sym = self._create_symbol_name(token)
            args.append(f'publicTrade.{sym}')

        sub_msg = {"op": "subscribe", "args": args}
        await self.subscribe(endpoint='linear', sub_msg=sub_msg)
