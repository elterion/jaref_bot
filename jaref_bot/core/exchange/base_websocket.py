import asyncio
import orjson
import json
import websockets
import socket
from websockets.exceptions import ConnectionClosedError, ConnectionClosed
import logging
from abc import ABC, abstractmethod
from jaref_bot.config.endpoints import WS_ENDPOINTS
from jaref_bot.core.exceptions.exchange import UnknownEndpointError
import redis
import pickle
from datetime import datetime

logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger()

class BaseWebSocketClient(ABC):
    def __init__(self, demo: bool = False):
        self.demo = demo
        self.connections = {}  # Хранение активных соединений
        self.subscriptions = {}
        self.lock = asyncio.Lock()
        self.db_client = redis.Redis(db=0, decode_responses=True)
        self.connected = False
        self.coin_info = self._load_token_info()

    def _load_token_info(self):
        with open("./data/coin_information.pkl", "rb") as f:
            coin_information = pickle.load(f)
        return coin_information

    def _chunk_list(self, lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    def get_endpoint(self, stream_type):
        if stream_type in ('order', 'position'):
            endpoint = 'private'
        elif stream_type == 'spot':
            endpoint = 'spot'
        elif stream_type in ('orderbook', 'ticker'):
            endpoint = 'linear'
        else:
            raise UnknownEndpointError
        return endpoint

    def load_urls(self, endpoint):
        mode = 'demo' if self.demo else 'prod'
        return WS_ENDPOINTS[self.exchange][endpoint][mode]

    @abstractmethod
    async def authenticate(self, stream_type: str):
        pass

    async def connect(self, stream_type: str, auth_required=False):
        ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        endpoint = self.get_endpoint(stream_type)
        url = self.load_urls(endpoint)
        if not url:
            raise ValueError(f"URL для типа стрима '{stream_type}' не найден")
        reconnect_delay = 1  # Начальная задержка

        while True:  # Бесконечный цикл переподключения
            try:
                logger.debug(f'Connecting to {url}')
                async with websockets.connect(url, ping_interval=15, ping_timeout=20) as ws:
                    self.connections[endpoint] = ws
                    logger.debug(f'Connected to {self.exchange}')
                    reconnect_delay = 1  # Сброс задержки после успешного подключения

                    if auth_required:
                        logger.debug(f'Sending auth request')
                        await self.authenticate(endpoint)

                    await self.resubscribe(endpoint)
                    logger.debug(f'Listening... {url}')
                    await self.listen(ws, stream_type)

            except (ConnectionClosedError, ConnectionClosed) as err:
                logger.error(f'{self.exchange} connection closed!')
            except socket.gaierror as err:
                logger.error(f'{ct} Отсутствует подключение')

            self.connected = False

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

            logger.error(f'{ct} Переподключение.')

    async def resubscribe(self, endpoint: str):
        for sub_msg in self.subscriptions.get(endpoint, []):
            if sub_msg:
                await self.subscribe(endpoint, sub_msg)

    async def subscribe(self, endpoint: str, sub_msg):
        async with self.lock:
            await asyncio.sleep(0.2 if self.connected else 2)

            if endpoint not in self.subscriptions:
                self.subscriptions[endpoint] = []
                logger.debug(f'Adding new subscription: {endpoint}')
            if not sub_msg in self.subscriptions[endpoint]:
                self.subscriptions[endpoint].append(sub_msg)

            await self.connections[endpoint].send(json.dumps(sub_msg))

    async def listen(self, ws, stream_type: str):
        async for msg in ws:
            try:
                data = orjson.loads(msg)
                await self.default_handler(stream_type, data)
            except orjson.JSONDecodeError:
                logger.error(f"Failed to parse message: {msg}")

    async def default_handler(self, stream_type, msg):
        print(f'default handler. {stream_type=}, {msg=}')
