import aiohttp
import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
import logging

# Настройка логгирования
logging.basicConfig(level=logging.INFO, 
                    format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger('aiohttp').setLevel('ERROR')
logging.getLogger('asyncio').setLevel('ERROR')
logger = logging.getLogger()


class ExchangeRestAPI(ABC):
    interval_dic = {'bybit': {'1m': 1, '5m': 5, '15m': 15, '30m': 30,
                            '1h': 60, '4h': 240, '1d': 'D'},
                    'okx': {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', 
                            '1h': '1H', '4h': '4H', '1d': '1Dutc'},
                    'gate': {'1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m', 
                            '1h': '1h', '4h': '4h', '1d': '1d'}}

    def __init__(self, category):
        self.category = category
        assert self.category in ('spot', 'linear'), 'category should be "spot" or "linear"'
    
    async def _send_request(self, session, endpoint, params=None):
        params = params if params else {}
        async with session.get(self.BASE_URL + endpoint, params=params) as response:
            return await response.json()
                    
    @abstractmethod
    async def _get_exchange_name(self):
        pass

    @abstractmethod
    async def get_tickers(self, session):
        pass

    @abstractmethod
    async def _get_candles_endpoint(self):
        pass

    @abstractmethod
    async def _get_candles_params_to_send(self):
        pass

    @abstractmethod
    async def _get_candles_workflow_params(self):
        pass

    async def get_candles(self, session, symbol, interval, n_iters=1, end_date=None):
        # assert interval in ('1m', '5m', '15m', '30m', '1h', '4h', '1d'), "possible interval values: '1m', '5m', '15m', '30m', '1h', '4h', '1d'"
        
        exc = self._get_exchange_name()
        endpoint = self._get_candles_endpoint()

        params = self._get_candles_params_to_send()
        wf_prams = self._get_candles_workflow_params()

        cols = wf_prams['cols']
        cols_to_drop = wf_prams['cols_to_drop']
        time_unit = wf_prams['time_unit']
        
        if exc == 'bybit':
            params['symbol'] = symbol + 'USDT'
            params['interval'] = self.interval_dic['bybit'][interval]
        
        elif exc == 'okx':
            params['instId'] = symbol + '-USDT'
            if self.category == 'linear':
                params['instId'] += '-SWAP'
            params['interval'] = self.interval_dic['okx'][interval]
            n_iters *= 10
        elif exc == 'gate':
            if self.category == 'spot':
                params['currency_pair'] = symbol + '_USDT'
            elif self.category == 'linear':
                params['contract'] = symbol + '_USDT'
            params['interval'] = self.interval_dic['gate'][interval]
            n_iters *= 10
        
        if end_date is None:
            end_date = ''

        hist_df = pd.DataFrame(columns=cols)

        try:
            for _ in range(n_iters):
                # logger.info(f"Fetching data for {symbol} from {self.__class__.__name__}...")
                
                data = await self._send_request(session, endpoint=endpoint, params=params)
                
                if not data:
                    logger.warning(f"No data returned for {symbol}.")
                    break

                if exc == 'bybit':
                    hist = data['result']['list']
                    end_date = str(int(hist[-1][0]) - 1)
                    params['end'] = end_date
                elif exc == 'okx':
                    hist = data['data']
                    end_date = str(int(hist[-1][0]) - 1)
                    params['after'] = end_date
                elif exc == 'gate':
                    hist = data

                    if self.category == 'spot':
                        end_date = str(int(hist[0][0]) - 1)
                    elif self.category == 'linear':
                        end_date = str(int(hist[0]['t']) - 1)

                    params['to'] = end_date

                if exc == 'gate' and self.category == 'linear':
                    tdf = pd.DataFrame(hist)
                else:
                    tdf = pd.DataFrame(hist, columns=cols)
                
                hist_df = pd.concat([hist_df if not hist_df.empty else None, tdf], 
                    ignore_index=True)
                
        except KeyError as e:
            logger.warning(f'No data for {exc} {self.category}')
        except Exception as e:
            logger.error(f"{exc} {self.category}. Error fetching data: {e}")
        
        if cols_to_drop:
            try:
                hist_df.drop(cols_to_drop, axis=1, inplace=True)
            except KeyError:
                logger.error(f"Error drop columns. {exc=}, {self.category=}")
                raise KeyError
        
        if exc == 'gate':
            hist_df.rename(columns={'o': 'Open', 'v': 'Volume', 't': 'Date',
                'c': 'Close', 'l': 'Low', 'h': 'High', 'sum': 'Turnover'}, 
                inplace=True)

        hist_df[['Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']] = hist_df[
            ['Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']].astype(float)

        hist_df['Date'] = pd.to_datetime(hist_df['Date'].astype(float), unit=time_unit)
        hist_df.index = hist_df['Date']
        hist_df.drop('Date', axis=1, inplace=True)
        hist_df.index = hist_df.index.tz_localize('UTC').tz_convert('Europe/Moscow')
        hist_df.sort_index(inplace=True)
        hist_df['Exchange'] = exc
        hist_df['Market_type'] = self.category

        if exc == 'okx' and self.category == 'linear':
            hist_df['Volume'] *= 100


        return hist_df[['Open', 'High', 'Low', 'Close', 'Volume', 
                            'Turnover', 'Exchange', 'Market_type']]

        
class BybitRestAPI(ExchangeRestAPI):
    BASE_URL = "https://api.bybit.com"

    def _get_exchange_name(self):
        return 'bybit'

    def _get_candles_endpoint(self):
        return '/v5/market/kline'

    def _get_candles_params_to_send(self):
        return {'category': self.category, 'limit': 1000}

    def _get_candles_workflow_params(self):
        cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
        cols_to_drop = []
        time_unit = 'ms'
        return {'cols': cols, 'cols_to_drop': cols_to_drop, 'time_unit': time_unit}

    async def get_tickers(self, session):
        endpoint = "/v5/market/tickers"
        params = {"category": self.category}
        exchange_rates = {}
        
        data = await self._send_request(session, endpoint=endpoint, params=params)

        for ticker in data['result']['list']:
            vol24h = int(float(ticker['turnover24h']))
            
            if ticker['symbol'].endswith('USDT') and vol24h > 1_000:
                sym = ticker['symbol'][:-4] + '_' + ticker['symbol'][-4:]
                
                if self.category == 'linear':
                    next_ft = datetime.fromtimestamp(int(ticker['nextFundingTime'][:-3])).strftime('%Y-%m-%d %H:%M')
                
                    exchange_rates[sym] = {'bid_price': float(ticker['bid1Price']), 'ask_price': float(ticker['ask1Price']),
                                           'bid_size': float(ticker['bid1Size']), 'ask_size': float(ticker['ask1Size']),
                                           'last_price': float(ticker['lastPrice']), 'index_price': float(ticker['indexPrice']),
                                           'vol24h_usdt': float(ticker['turnover24h']), 
                                           'funding_rate': float(ticker['fundingRate']), 'next_fund_time': next_ft}
                elif self.category == 'spot':
                    exchange_rates[sym] = {'bid_price': float(ticker['bid1Price']), 'ask_price': float(ticker['ask1Price']),
                                           'bid_size': float(ticker['bid1Size']), 'ask_size': float(ticker['ask1Size']),
                                           'last_price': float(ticker['lastPrice']), 'vol24h_usdt': vol24h}
        return exchange_rates


class OKXRestAPI(ExchangeRestAPI):
    BASE_URL = 'https://www.okx.com'

    def _get_exchange_name(self):
        return 'okx'

    def _get_candles_endpoint(self):
        return '/api/v5/market/history-candles'

    def _get_candles_params_to_send(self):
        return {}

    def _get_candles_workflow_params(self):
        cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', '0', 'Turnover', '1']
        cols_to_drop = ['0', '1']
        time_unit = 'ms'
        return {'cols': cols, 'cols_to_drop': cols_to_drop, 'time_unit': time_unit}


    async def get_tickers(self, session):
        category = self.category.upper() if self.category =='spot' else 'SWAP'
        
        endpoint = '/api/v5/market/tickers'
        params = {"instType": category}
        exchange_rates = {}
        
        data = await self._send_request(session, endpoint=endpoint, params=params)
        
        for ticker in data['data']:
            vol24h = int(float(ticker['volCcy24h']))
            coins = ticker['instId'].split('-')
            
            if coins[1] == 'USDT' and vol24h > 1_000:
                sym = coins[0] + '_' + coins[1]
                exchange_rates[sym] = {'bid_price': float(ticker['bidPx']), 'ask_price': float(ticker['askPx']),
                               'bid_size': float(ticker['bidSz']), 'ask_size': float(ticker['askSz']),
                               'last_price': float(ticker['last']), 'vol24h_usdt': vol24h}
        return exchange_rates


class GateIORestAPI(ExchangeRestAPI):
    BASE_URL = 'https://api.gateio.ws'

    def _get_exchange_name(self):
        return 'gate'

    def _get_candles_endpoint(self):
        if self.category == 'spot':
            return '/api/v4/spot/candlesticks'
        elif self.category == 'linear':
            return '/api/v4/futures/usdt/candlesticks'

    def _get_candles_params_to_send(self):
        return {'Accept': 'application/json', 'Content-Type': 'application/json'}

    def _get_candles_workflow_params(self):
        if self.category == 'spot':
            cols = ['Date', 'Volume', 'Close', 'High', 'Low', 'Open', 'Turnover', '0']
            cols_to_drop = ['0']
        elif self.category == 'linear':
            cols = ['Date', 'Volume', 'Close', 'High', 'Low', 'Open', 'Turnover']
            cols_to_drop = []
        time_unit = 's'
        return {'cols': cols, 'cols_to_drop': cols_to_drop, 'time_unit': time_unit}


    async def get_tickers(self, session):
        if self.category == 'spot':
            endpoint = '/api/v4/spot/tickers'
        elif self.category == 'linear':
            endpoint = '/api/v4/futures/usdt/tickers'
        params = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        exchange_rates = {}
        
        data = await self._send_request(session, endpoint=endpoint, params=params)
                
        for ticker in data:
            if self.category == 'linear':
                vol24h = int(float(ticker['volume_24h_quote']))
            elif self.category == 'spot':
                vol24h = int(float(ticker['quote_volume']))
            sym = ticker['contract'] if self.category == 'linear' else ticker['currency_pair']
            
            if sym.endswith('USDT') and vol24h > 1_000:
                if self.category == 'spot':
                    try:
                        exchange_rates[sym] = {'bid_price': float(ticker['highest_bid']), 'ask_price': float(ticker['lowest_ask']),
                                       'last_price': float(ticker['last']), 'vol24h_usdt': vol24h}
                    except ValueError:
                        continue
                elif self.category == 'linear':
                    try:
                        exchange_rates[sym] = {'bid_price': float(ticker['highest_bid']), 'ask_price': float(ticker['lowest_ask']),
                                       'bid_size': float(ticker['highest_size']), 'ask_size': float(ticker['lowest_size']),
                                       'last_price': float(ticker['last']), 'index_price': float(ticker['index_price']),
                                       'vol24h_usdt': vol24h, 'funding_rate': float(ticker['funding_rate'])}
                    except ValueError:
                        continue
        return exchange_rates



class ExchangeManager:
    def __init__(self):
        self.exchanges = {}

    def add_market(self, name: str, exchange: ExchangeRestAPI):
        self.exchanges[name] = exchange

    async def get_prices(self):
        async with aiohttp.ClientSession() as session:
            tasks = {
                name: asyncio.create_task(exchange.get_tickers(session))
                for name, exchange in self.exchanges.items()
            }
            
            results = {}
            for name, task in tasks.items():
                results[name] = await task
            
            return results


    async def get_candles(self, symbol, interval, n_iters=1):
        async with aiohttp.ClientSession() as session:
            tasks = {
                name: exchange.get_candles(session, symbol, interval, n_iters)
                for name, exchange in self.exchanges.items()
            }
            return await asyncio.gather(*tasks.values(), return_exceptions=True)