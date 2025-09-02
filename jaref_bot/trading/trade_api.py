from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config import credentials as cr
from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI, OKXRestAPI, GateIORestAPI

from jaref_bot.core.exceptions.trading import SetLeverageError, PlaceOrderError, NoSuchOrderError

from datetime import datetime, UTC
import time
import requests
import hmac
import hashlib
import base64
import json
from decimal import Decimal, getcontext, InvalidOperation
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger('aiohttp').setLevel('ERROR')
logging.getLogger('asyncio').setLevel('ERROR')
logger = logging.getLogger()

# Устанавливаем точность округления Decimal чисел
getcontext().prec = 8

class BaseTrade:
    def place_market_order(self, market_type, symbol, side, qty, ct_val=1,
                     margin_mode='isolated', stop_loss=None, **kwargs):
        market_type = market_type.lower()
        side = side.lower()
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'
        assert side in ('buy', 'sell'), 'side should be "buy" or "sell"'

        response = self._place_market_order(market_type=market_type, symbol=symbol, side=side,
            qty=qty, stop_loss=stop_loss, ct_val=ct_val, margin_mode=margin_mode)
        return response

    def place_limit_order(self, market_type, symbol, side, qty, price, ct_val=1,
                     margin_mode='isolated', stop_loss=None, **kwargs):
        market_type = market_type.lower()
        side = side.lower()
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'
        assert side in ('buy', 'sell'), 'side should be "buy" or "sell"'

        response = self._place_limit_order(market_type=market_type, symbol=symbol, side=side,
            qty=qty, price=price, stop_loss=stop_loss, ct_val=ct_val, margin_mode=margin_mode)
        return response

    def place_conditional_order(self, market_type, symbol, side, qty, price,
            trigger_price=None, ct_val=1, margin_mode='isolated', stop_loss=None, **kwargs):
        market_type = market_type.lower()
        side = side.lower()
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'
        assert side in ('buy', 'sell'), 'side should be "buy" or "sell"'

        response = self._place_conditional_order(market_type=market_type, symbol=symbol, side=side,
            qty=qty, price=price, trigger_price=trigger_price, stop_loss=stop_loss, ct_val=ct_val,
            margin_mode=margin_mode)
        return response

    def cancel_order(self, market_type, symbol, order_id, **kwargs):
        market_type = market_type.lower()
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'

        response = self._cancel_order(market_type=market_type,
                                      symbol=symbol,
                                      order_id=order_id)
        return response

    def set_leverage(self, market_type, symbol, lever):
        response = self._set_leverage(market_type=market_type, symbol=symbol, lever=lever)
        return response

    def get_position(self, market_type, symbol, order_type, **kwargs):
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'
        assert order_type in ('market', 'limit'), 'order_type should be "market" or "limit"'
        return self._get_position(market_type, symbol, order_type, **kwargs)

    def get_order(self, symbol, market_type, order_id, **kwargs):
        assert market_type in ('linear', 'spot'), 'market_type should be "linear" or "spot"'
        return self._get_order(symbol=symbol, market_type=market_type, order_id=order_id, **kwargs)


class BybitTrade(BaseTrade):
    EXCHANGE = 'bybit'

    def __init__(self, demo=False):
        self.demo = demo
        if self.demo:
            self.api_key = cr.BYBIT_DEMO_API_KEY
            self.secret_key = cr.BYBIT_DEMO_SECRET_KEY
            self.main_url = 'https://api-demo.bybit.com'
            logger.debug('Demo-mode is on!')
        else:
            self.api_key = cr.BYBIT_API_KEY
            self.secret_key = cr.BYBIT_SECRET_KEY
            self.main_url = 'https://api.bybit.com'
            logger.debug('Demo-mode is off!')

        self.fees = {'linear_market': 0.001, 'linear_limit': 0.00036}

    def _create_symbol_name(self, symbol):
        return ''.join(symbol.split('_'))

    def hashing(self, query):
        return hmac.new(self.secret_key.encode('utf-8'), query.encode('utf-8'), hashlib.sha256).hexdigest()

    def _prepare_headers(self, ts, sign):
        headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': str(ts),
            'X-BAPI-SIGN': sign,
            'X-BAPI-RECV-WINDOW': str(5000),
            'Content-Type': 'application/json'
        }
        return headers

    def _place_order(self, market_type,
                      symbol,
                      side,
                      order_type,
                      qty,
                      price=None,
                      trigger_price=None,
                      stop_loss=None,
                      **kwargs):
        url = self.main_url + '/v5/order/create'

        sym = self._create_symbol_name(symbol)
        side = side.capitalize()
        price = price if price else ''
        curr_time = int(datetime.now().timestamp()*1000)
        data = '{' + f'"category": "{market_type}","symbol": "{sym}","side": "{side}","orderType": "{order_type}","qty": "{qty}","price": "{price}"' + '}'
        if trigger_price:
            if side == 'Buy':
                data = data[:-1] + f',"triggerPrice": "{trigger_price}","triggerDirection": "1"' + '}'
            elif side == 'Sell':
                data = data[:-1] + f',"triggerPrice": "{trigger_price}", "triggerDirection": "2"' + '}'
        if stop_loss:
            data = data[:-1] + f',"stopLoss": "{stop_loss}"' + '}'

        sign = self.hashing(str(curr_time) + self.api_key + '5000' + data)
        headers = self._prepare_headers(ts=curr_time, sign=sign)
        response = requests.post(url=url, headers=headers, data=data).json()

        order_id = response.get('result', {}).get('orderId')
        if order_id:
            logger.debug(f'[PLACE ORDER] Bybit {market_type=} {sym=} {order_id=}')
            return order_id
        else:
            logger.error(f'[ORDER ERROR] Bybit {market_type=} {sym=}')
            raise PlaceOrderError(f'При постановке ордера на бирже Bybit возникла ошибка: {response['retMsg']}')

    def _place_market_order(self, market_type, symbol, side, qty, stop_loss=None, **kwargs):
        return self._place_order(market_type=market_type,
                      symbol=symbol,
                      side=side,
                      order_type='market',
                      qty=qty,
                      stop_loss=stop_loss)

    def _place_limit_order(self, market_type, symbol, side, qty, price, stop_loss=None, **kwargs):
        return self._place_order(market_type=market_type,
                      symbol=symbol,
                      side=side,
                      order_type='limit',
                      qty=qty,
                      price=price,
                      stop_loss=stop_loss)

    def _place_conditional_order(self, market_type, symbol, side, qty, price, trigger_price,
                                 stop_loss=None, **kwargs):
        return self._place_order(market_type=market_type,
                      symbol=symbol,
                      side=side,
                      order_type='limit',
                      qty=qty,
                      price=price,
                      trigger_price=trigger_price,
                      stop_loss=stop_loss)

    def _cancel_order(self, market_type, symbol, order_id, **kwargs):
        url = self.main_url + '/v5/order/cancel'
        sym = self._create_symbol_name(symbol)
        curr_time = int(datetime.now().timestamp()*1000)
        data = '{' + f'"category": "{market_type}","symbol": "{sym}","orderId": "{order_id}"' + '}'
        sign = self.hashing(str(curr_time) + self.api_key + '5000' + data)
        headers = self._prepare_headers(ts=curr_time, sign=sign)

        response = requests.post(url=url, headers=headers, data=data).json()
        order_id = response.get('result', {}).get('orderId')
        if response.get('retCode') == 0 and response.get('retMsg') == 'OK':
            logger.debug(f'[CANCEL ORDER] Bybit {market_type=} {sym=} {order_id=}')
            return order_id
        else:
            logger.error(f'[ORDER ERROR] Bybit {market_type=} {sym=}')
            raise NoSuchOrderError(f'При отмене ордера на бирже Bybit возникла ошибка: {response['retMsg']}')

    def _set_leverage(self, market_type, symbol, lever):
        url = self.main_url + '/v5/position/set-leverage'

        sym = self._create_symbol_name(symbol)
        curr_time = int(datetime.now().timestamp()*1000)
        data = '{' + f'"category": "{market_type}","symbol": "{sym}","buyLeverage": "{lever}","sellLeverage": "{lever}"' + '}'
        sign = self.hashing(str(curr_time) + self.api_key + '5000' + data)
        headers = self._prepare_headers(ts=curr_time, sign=sign)

        response = requests.post(url=url, headers=headers, data=data).json()
        if response['retCode'] == 0 or (response['retCode'] == 110043
                                    and response['retMsg'] == 'leverage not modified'):
            logger.debug(f'Bybit {sym} leverage successfully set to {lever}')
        else:
            logger.error(f'[LEVERAGE ERROR] Bybit {sym}')
            raise SetLeverageError(f'При изменении плеча на бирже Bybit возникла ошибка: {response['retMsg']}')
        return response

    def _position_handler(self, resp, market_type, order_type):
        # print(resp)
        data = resp['result']['list'][0]

        token = data['symbol']
        base, quote = token[:-4], token[-4:]
        token = base + '_' + quote

        leverage = Decimal(data['leverage'])
        price = Decimal(data['avgPrice'])
        usdt_amount = Decimal(data['positionBalance']) # Начальная стоимость в usdt. Да, проверил.
        side = data['side'].lower()
        size = Decimal(data['size'])

        fee_perc = Decimal(self.fees[market_type + '_' + order_type])
        fee = (usdt_amount * fee_perc).normalize()

        realized_pnl = Decimal(data['curRealisedPnl'])
        unrealized_pnl = Decimal(data['unrealisedPnl'])

        return {'exchange': 'bybit', 'market_type': market_type, 'order_type': order_type,
                'token': token, 'leverage': leverage, 'price': price,
                'usdt_amount': usdt_amount, 'qty': size, 'order_side': side, 'fee': fee,
                'realized_pnl': realized_pnl, 'unrealized_pnl': unrealized_pnl}

    def _get_position(self, market_type, symbol, order_type, **kwargs):
        """
        Возвращает информацию по открытой позиции
        """
        url = self.main_url + '/v5/position/list'

        sym = self._create_symbol_name(symbol)
        curr_time = int(datetime.now().timestamp()*1000)
        query = f'category={market_type}&symbol={sym}'

        sign = self.hashing(str(curr_time) + self.api_key + '5000' + query)
        headers = self._prepare_headers(ts=curr_time, sign=sign)

        url = f'{url}?{query}'
        response = requests.get(url=url, headers=headers).json()

        # return response
        try:
            return self._position_handler(response, market_type, order_type)
        except InvalidOperation:
            return None

    def _order_handler(self, resp, market_type):
        data = resp['result']['list'][0]

        token = data['symbol']
        if token.endswith('USDT'):
            base, quote = token[:-4], token[-4:]
            token = base + '_' + quote
        else:
            raise NotImplementedError

        order_type = data['orderType'].lower()
        status = data['orderStatus'].lower()
        price = Decimal(data['avgPrice']) # Цена по которой ордер сматчился
        limit_price = Decimal(data['price']) # Заявочная цена для лимитного ордера
        usdt_amount = Decimal(data['cumExecValue'])
        side = data['side'].lower()
        size = Decimal(data['cumExecQty'])
        fee = Decimal(data['cumExecFee'])

        return {'exchange': 'bybit', 'market_type': market_type, 'order_type': order_type,
                'status': status, 'token': token, 'price': price, 'limit_price': limit_price,
                'usdt_amount': usdt_amount, 'qty': size, 'order_side': side, 'fee': fee}

    def _get_order(self, market_type, order_id, **kwargs):
        url = self.main_url + '/v5/order/realtime'

        curr_time = int(datetime.now().timestamp()*1000)
        query = f'category={market_type}&orderId={order_id}'

        sign = self.hashing(str(curr_time) + self.api_key + '5000' + query)
        headers = self._prepare_headers(ts=curr_time, sign=sign)

        url = f'{url}?{query}'
        response = requests.get(url=url, headers=headers).json()

        # return response
        try:
            return self._order_handler(response, market_type)
        except InvalidOperation:
            return None

    def _get_order_history(self, market_type, settle_coin, **kwargs):
        url = self.main_url + '/v5/order/realtime'

        curr_time = int(datetime.now().timestamp()*1000)
        query = f'category={market_type}&settleCoin={settle_coin}&limit=50'

        sign = self.hashing(str(curr_time) + self.api_key + '5000' + query)
        headers = self._prepare_headers(ts=curr_time, sign=sign)

        url = f'{url}?{query}'
        response = requests.get(url=url, headers=headers).json()

        return response
        try:
            return self._order_handler(response, market_type)
        except InvalidOperation:
            return None


class OkxClient:
    EXCHANGE = 'okx'

    def __init__(self, demo=True):
        self.demo = demo
        if self.demo:
            self.api_key = cr.OKX_DEMO_API_KEY
            self.secret_key = cr.OKX_DEMO_SECRET_KEY
            self.passphrase = cr.OKX_DEMO_PASSPHRASE
            logger.debug('Demo-mode is on!')
        else:
            self.api_key = cr.OKX_API_KEY
            self.secret_key = cr.OKX_SECRET_KEY
            self.passphrase = cr.OKX_PASSPHRASE
            logger.debug('Demo-mode is off!')

        self.domain = 'https://www.okx.cab'

    def _create_symbol_name(self, market_type, symbol):
        sym = '-'.join(symbol.split('_'))
        if market_type == 'linear':
            sym += '-SWAP'
        return sym

    def _parse_params_to_str(self, params):
        url = '?'
        for key, value in params.items():
            if(value != ''):
                url = url + str(key) + '=' + str(value) + '&'
        url = url[0:-1]
        return url

    def _pre_hash(self, timestamp, method, request_path, body):
        return str(timestamp) + str.upper(method) + request_path + body

    def _sign(self, message):
        mac = hmac.new(bytes(self.secret_key, encoding='utf-8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d)

    def _get_header(self, sign, timestamp):
        header = dict()
        header['Content-Type'] = 'application/json'
        header['OK-ACCESS-KEY'] = self.api_key
        header['OK-ACCESS-SIGN'] = sign
        header['OK-ACCESS-TIMESTAMP'] = str(timestamp)
        header['OK-ACCESS-PASSPHRASE'] = self.passphrase
        header['x-simulated-trading'] = '1' if self.demo else '0'

        return header

    def _get_header_no_sign(self):
        header = dict()
        header['Content-Type'] = 'application/json'
        header['x-simulated-trading'] = '1' if self.demo else '0'

        return header

    def _get_time(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        t = now.isoformat("T", "milliseconds")
        return t + 'Z'

    def _request(self, method, request_path, params):
        if method == 'GET':
            request_path = request_path + self._parse_params_to_str(params)
        timestamp = self._get_time()
        body = json.dumps(params) if method == 'POST' else ""

        if self.api_key is not None:
            sign = self._sign(self._pre_hash(timestamp, method, request_path, str(body)))
            header = self._get_header(sign, timestamp)
        else:
            header = self._get_header_no_sign()

        if method == 'GET':
            response = requests.get(self.domain+request_path, headers=header)
        elif method == 'POST':
            response = requests.post(self.domain + request_path, data=body, headers=header)
        return response.json()

    def _request_without_params(self, method, request_path):
        return self._request(method, request_path, {})

    def _request_with_params(self, method, request_path, params):
        return self._request(method, request_path, params)


class OkxTrade(OkxClient, BaseTrade):
    def __init__(self, demo=True):
        OkxClient.__init__(self, demo=demo)

    def _place_order(self, market_type,
                     symbol,
                     side,
                     order_type,
                     qty,
                     price=None,
                     stop_loss=None,
                     ct_val=1,
                     margin_mode='isolated',
                     **kwargs):
        """
        docs: https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order

        :params market_type: тип рынка ('spot', 'linear')
        :params symbol: нужный токен ('BTC', 'ADA', 'XRP' etc)
        :params side: направление открытия сделки ('buy', 'sell')
        :params order_type: тип ордера ('market', 'limit')
        :params qty: количество монет в базовой валюте
        :params ct_val: размер контракта для фьючерсного рынка
        :params margin_mode: вид маржи ('isolated', 'cross')
        """
        sym = self._create_symbol_name(market_type, symbol)
        price = price if price else ''
        if isinstance(price, Decimal):
            price = price.normalize().to_eng_string()
        body = {
            'instId': sym,
            'tdMode': margin_mode,   # Margin mode 'cross', 'isolated'; Non-Margin mode 'cash'
            'side': side,
            'ordType': order_type.lower(),
            'sz': qty,
            'px': price
        }
        if market_type == 'spot':
            body['tdMode'] = 'cash' # При торговле на споте 'tdMode' должен быть 'cash'
            body['tgtCcy'] = 'base_ccy' # Считаем qty считаем в базовой валюте
        elif market_type == 'linear':
            body['sz'] /= ct_val
            # body['posSide'] = 'net'
            if isinstance(ct_val, Decimal):
                body['sz'] = body['sz'].to_eng_string()
        else:
            raise Exception('Unknown market_type. Should be "linear" or "spot".')

        if stop_loss:
            if isinstance(stop_loss, Decimal):
                stop_loss = stop_loss.to_eng_string()
            body['attachAlgoOrds'] = {'slTriggerPx': stop_loss, 'slOrdPx': '-1'}

        # print(body)
        response = self._request_with_params(method='POST',
                                             request_path='/api/v5/trade/order',
                                             params=body)

        order_id = response.get('data', {})[0].get('ordId', None)

        if order_id:
            logger.debug(f'[PLACE ORDER] Okx {market_type=} {sym=} {order_id=}')
        else:
            logger.error(f'[ORDER ERROR] Okx {market_type=} {sym=}')
            raise PlaceOrderError(f'При постановке ордера на бирже Okx возникла ошибка: {response['data'][0]['sMsg']}')

        return order_id

    def _place_market_order(self, market_type, symbol, side, qty, ct_val,
                            stop_loss=None, margin_mode='isolated'):
        return self._place_order(market_type=market_type,
                                        symbol=symbol,
                                        side=side,
                                        order_type='market',
                                        qty=qty,
                                        stop_loss=stop_loss,
                                        ct_val=ct_val,
                                        margin_mode=margin_mode)

    def _place_limit_order(self, market_type, symbol, side, qty, price, ct_val,
                           stop_loss=None, margin_mode='isolated'):
        return self._place_order(market_type=market_type,
                                        symbol=symbol,
                                        side=side,
                                        order_type='limit',
                                        qty=qty,
                                        price=price,
                                        stop_loss=stop_loss,
                                        ct_val=ct_val,
                                        margin_mode=margin_mode)

    def _cancel_order(self, symbol, market_type, order_id='', **kwargs):
        sym = self._create_symbol_name(market_type, symbol)
        body = {
            'instId': sym,
            'ordId': order_id
        }

        response = self._request_with_params(method='POST',
                                             request_path='/api/v5/trade/cancel-order',
                                             params=body)

        order_id = response.get('data', [])[0].get('ordId', None)
        if response.get('code') == '0':
            logger.debug(f'[CANCEL ORDER] Okx {market_type=} {sym=} {order_id=}')
            return order_id
        else:
            logger.error(f'[ORDER ERROR] Okx {market_type=} {sym=}')
            raise NoSuchOrderError(f'При отмене ордера на бирже Okx возникла ошибка: {response['msg']}')

    def _set_leverage(self, market_type, symbol, lever, margin_mode='isolated'):
        assert margin_mode in ('isolated', 'cross'), 'margin_mode should be "isolated" or "cross"'
        sym = self._create_symbol_name(market_type, symbol)

        for ps in ('long', 'short'):
            body = {
                'instId': sym,
                'lever': str(lever),
                'mgnMode': margin_mode,
                # 'posSide': ps
            }
            response = self._request_with_params(method='POST',
                                                request_path='/api/v5/account/set-leverage',
                                                params=body)
            if response['code'] == '0':
                logger.debug(f'Okx {sym} leverage successfully set to {lever}')
            else:
                logger.error(f'[LEVERAGE ERROR] Okx {sym}')
                logger.error(f'{body=}')
                raise SetLeverageError(f'При изменении плеча на бирже Okx возникла ошибка: {response['msg']}')
        return response

    def _position_handler(self, resp, market_type, order_type, ct_val):
        try:
            data = resp['data'][0]
        except IndexError:
            return None

        token = data['instId']
        syms = token.split('-')
        token = syms[0] + '_' + syms[1]

        leverage = Decimal(data['lever'])
        price = Decimal(data['avgPx'])

        side = 'buy' if float(data['pos']) > 0 else 'sell'
        qty = abs(Decimal(data['pos']) * ct_val)
        usdt_amount = Decimal(data['margin'])
        fee = abs(Decimal(data['fee']))

        realized_pnl = Decimal(data['realizedPnl']).normalize()
        unrealized_pnl = Decimal(data['uplLastPx']).normalize()
        curr_position_value = Decimal(data['notionalUsd']).normalize()

        return {'exchange': 'okx', 'market_type': market_type, 'order_type': order_type,
                'token': token, 'leverage': leverage, 'price': price,
                'usdt_amount': usdt_amount, 'qty': qty, 'order_side': side, 'fee': fee,
                'realized_pnl': realized_pnl, 'unrealized_pnl': unrealized_pnl,
                'curr_position_value': curr_position_value}

    def _get_position(self, market_type, symbol, order_type, ct_val):
        sym = self._create_symbol_name(market_type, symbol)
        mt = 'SWAP' if market_type == 'linear' else 'SPOT'
        path = f'/api/v5/account/positions?instType={mt}&instId={sym}'

        response = self._request_without_params(method='GET',
                    request_path=path)
        # return response
        try:
            return self._position_handler(response, market_type, order_type, ct_val)
        except InvalidOperation:
            return None

    def _order_handler(self, resp, market_type, ct_val):
        data = resp['data'][0]

        line = data['instId']
        syms = line.split('-')
        base, quote = syms[0], syms[1]
        token = base + '_' + quote

        status = data['state']
        order_type = data['ordType']

        price = Decimal(data['fillPx'])
        limit_price = Decimal(data['px'])

        side = data['side']
        qty = Decimal(data['sz']) * ct_val
        fee = abs(Decimal(data['fee']))
        usdt_amount = (qty * price).normalize()

        return {'exchange': 'okx', 'market_type': market_type, 'order_type': order_type,
                'status': status, 'token': token, 'price': price, 'limit_price': limit_price,
                'usdt_amount': usdt_amount, 'qty': qty, 'order_side': side, 'fee': fee}

    def _get_order(self, symbol, market_type, order_id, ct_val, **kwargs):
        sym = self._create_symbol_name(market_type, symbol)
        path = f'/api/v5/trade/order?ordId={order_id}&instId={sym}'
        response = self._request_without_params(method='GET',
                    request_path=path)
        # return response
        try:
            return self._order_handler(response, market_type, ct_val)
        except InvalidOperation:
            return None



    def close_position(self, market_type, symbol, margin_mode, posSide='', ccy='', autoCxl='', clOrdId='', tag=''):
        sym = self._create_symbol_name(market_type, symbol)
        params = {'instId': sym, 'mgnMode': margin_mode, 'posSide': posSide, 'ccy': ccy, 'autoCxl': autoCxl,
                  'clOrdId': clOrdId, 'tag': tag}
        return self._request_with_params('POST', request_path='/api/v5/trade/close-position', params=params)

    # def amend_order(self, instId, cxlOnFail='', ordId='', clOrdId='', reqId='', newSz='', newPx='', newTpTriggerPx='',
    #                 newTpOrdPx='', newSlTriggerPx='', newSlOrdPx='', newTpTriggerPxType='', newSlTriggerPxType='',
    #                 attachAlgoOrds=''):
    #     params = {'instId': instId, 'cxlOnFail': cxlOnFail, 'ordId': ordId, 'clOrdId': clOrdId, 'reqId': reqId,
    #               'newSz': newSz, 'newPx': newPx, 'newTpTriggerPx': newTpTriggerPx, 'newTpOrdPx': newTpOrdPx,
    #               'newSlTriggerPx': newSlTriggerPx, 'newSlOrdPx': newSlOrdPx, 'newTpTriggerPxType': newTpTriggerPxType,
    #               'newSlTriggerPxType': newSlTriggerPxType}
    #     params['attachAlgoOrds'] = attachAlgoOrds
    #     return self._request_with_params('POST', request_path='/api/v5/trade/amend-order', params=params)


class GateTrade(BaseTrade):
    EXCHANGE = 'gate'

    def __init__(self, demo=False):
        self.demo = demo
        if self.demo:
            self.api_key = cr.GATE_DEMO_API_KEY
            self.secret_key = cr.GATE_DEMO_SECRET_KEY
            logger.debug('Demo-mode is on!')
        else:
            self.api_key = cr.GATE_API_KEY
            self.secret_key = cr.GATE_SECRET_KEY
            logger.debug('Demo-mode is off!')

    def _create_symbol_name(self, symbol):
        return symbol

    def gen_sign(self, method, url, query_string=None, payload_string=None):
        key = self.api_key
        secret = self.secret_key

        t = int(time.time())
        m = hashlib.sha512()
        m.update((payload_string or "").encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}

    def _place_order(self, market_type,
                            symbol,
                            side,
                            order_type,
                            qty,
                            price=None,
                            stop_loss=None,
                            ct_val=1,
                            **kwargs):
        if self.demo:
            if market_type == 'linear':
                host = "https://fx-api-testnet.gateio.ws"
        else:
            if market_type == 'linear':
                host = "https://api.gateio.ws"

        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = '/futures/usdt/orders'
        query_param = ""

        size = qty / ct_val
        if side == 'sell':
            size = -size

        if isinstance(ct_val, Decimal):
            size = size.to_eng_string()

        symbol = self._create_symbol_name(symbol)
        if isinstance(price, Decimal):
            price = price.normalize().to_eng_string()
        else:
            price = str(price) if price else '0'
        tif = "ioc" if order_type == 'market' else 'gtc'
        body = '{"contract":"' + symbol + '", "size":' + size + ', "price": "' + price + '", "tif": "' + tif + '", "close": "false"}'
        sign_headers = self.gen_sign('POST', prefix + url, query_param, body)
        headers.update(sign_headers)

        response = requests.request('POST', host + prefix + url, headers=headers, data=body)

        try:
            response = response.json()
        except ValueError:
            logger.error(f'[ORDER ERROR] Gate {market_type=} {symbol=}')
            return response

        order_id = response.get('id')
        if order_id:
            logger.debug(f'[PLACE ORDER] Gate {market_type=} {symbol=} {order_id=}')
        else:
            logger.error(f'[ORDER ERROR] Gate {market_type=} {symbol=}')
            raise PlaceOrderError(f'При постановке ордера на бирже Gate возникла ошибка: {response['label']}, {response['message']}')
        return response

    def _place_market_order(self, market_type, symbol, side, qty, ct_val, stop_loss=None, **kwargs):
        return self._place_order(market_type=market_type,
                                        symbol=symbol,
                                        side=side,
                                        order_type='market',
                                        qty=qty,
                                        stop_loss=stop_loss,
                                        ct_val=ct_val)

    def _place_limit_order(self, market_type, symbol, side, qty, price, ct_val,
                           stop_loss=None, **kwargs):
        resp = self._place_order(market_type=market_type,
                                        symbol=symbol,
                                        side=side,
                                        order_type='limit',
                                        qty=qty,
                                        price=price,
                                        stop_loss=stop_loss,
                                        ct_val=ct_val)

        return resp
        return resp.get('id', None)

    def _cancel_order(self, market_type, order_id='', **kwargs):
        if self.demo:
            if market_type == 'linear':
                host = "https://fx-api-testnet.gateio.ws"
        else:
            if market_type == 'linear':
                host = "https://api.gateio.ws"

        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = f'/futures/usdt/orders/{order_id}'
        query_param = ""

        sign_headers = self.gen_sign('DELETE', prefix + url, query_param)
        headers.update(sign_headers)
        response = requests.request('DELETE', host + prefix + url + "?" + query_param,
                                    headers=headers).json()
        if response.get('status') == 'finished' and response.get('finish_as') == 'cancelled':
            return response.get('id')
        else:
            logger.error(f'[ORDER ERROR] Gate {market_type=}')
            raise NoSuchOrderError(f'При отмене ордера на бирже Gate.io возникла ошибка: {response}')

    def _set_leverage(self, market_type, symbol, lever):
        if self.demo:
            if market_type == 'linear':
                host = "https://fx-api-testnet.gateio.ws"
        else:
            if market_type == 'linear':
                host = "https://api.gateio.ws"

        symbol = self._create_symbol_name(symbol)
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = f'/futures/usdt/positions/{symbol}/leverage'
        query_param = f"leverage={lever}"

        sign_headers = self.gen_sign('POST', prefix + url, query_param)
        headers.update(sign_headers)
        response = requests.request('POST', host + prefix + url + "?" + query_param,
                                    headers=headers).json()
        if type(response) == dict and response.get('leverage'):
            logger.debug(f'Gate {symbol} leverage successfully set to {lever}')
        elif type(response) == list and response[0].get('leverage'):
            logger.debug(f'Gate {symbol} leverage successfully set to {lever}')
        else:
            logger.error(f'[LEVERAGE ERROR] Gate {symbol}')
            raise SetLeverageError(f'При изменении плеча на бирже Gate возникла ошибка: {response.get('label')}, {response.get('message')}')
        return response

    def _position_handler(self, resp, market_type, order_type, ct_val):
        if type(resp) == list:
            resp = resp[0]

        token = resp['contract']

        leverage = Decimal(resp['leverage'])
        price = Decimal(resp['entry_price'])
        side = 'buy' if float(resp['size']) > 0 else 'sell'
        qty = abs(Decimal(resp['size']) * ct_val)
        usdt_amount = Decimal(resp['margin'])
        curr_position_value = Decimal(resp['value'])
        fee = abs(Decimal(resp['pnl_fee']))
        realized_pnl = Decimal(resp['realised_pnl'])
        unrealized_pnl = Decimal(resp['unrealised_pnl'])

        return {'exchange': 'gate', 'market_type': market_type, 'order_type': order_type,
                'token': token, 'leverage': leverage, 'price': price,
                'usdt_amount': usdt_amount, 'qty': qty, 'order_side': side, 'fee': fee,
                'realized_pnl': realized_pnl, 'unrealized_pnl': unrealized_pnl,
                'curr_position_value': curr_position_value}
        # except TypeError:
        #     print(type(resp), resp)

    def _get_position(self, market_type, symbol, order_type, ct_val):
        if self.demo:
            if market_type == 'linear':
                host = "https://fx-api-testnet.gateio.ws"
        else:
            if market_type == 'linear':
                host = "https://api.gateio.ws"

        symbol = self._create_symbol_name(symbol)
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        url = f'/futures/usdt/positions/{symbol}'
        query_param = ""

        sign_headers = self.gen_sign('GET', prefix + url, query_param)
        headers.update(sign_headers)
        response = requests.request('GET', host + prefix + url + "?" + query_param,
                                    headers=headers).json()
        # return response
        try:
            return self._position_handler(response, market_type, order_type, ct_val)
        except InvalidOperation:
            return None

    def _order_handler(self, data, market_type, order_type, ct_val):
        token = data['contract']
        _id = data['id']
        status = data['status'].lower()
        price = Decimal(data['fill_price']) # Цена по которой ордер сматчился
        limit_price = Decimal(data['price']) # Заявочная цена для лимитного ордера
        side = 'buy' if float(data['size']) > 0 else 'sell'
        qty = abs(Decimal(data['size']) * ct_val)
        usdt_amount = Decimal(qty * price).normalize()
        fee = (Decimal(data['tkfr']) * usdt_amount).normalize()

        return {'exchange': 'gate', 'market_type': market_type, 'order_type': order_type,
                'status': status, 'token': token, 'price': price, 'limit_price': limit_price,
                'usdt_amount': usdt_amount, 'qty': qty, 'order_side': side, 'fee': fee,
                'id': _id}
