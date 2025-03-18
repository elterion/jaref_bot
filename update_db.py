from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI, OKXRestAPI, GateIORestAPI
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.utils.files import load_tokens_from_file
from jaref_bot.config.credentials import host, user, password, db_name

import pandas as pd
import numpy as np
from datetime import datetime
from time import sleep, time
import orjson
import redis
from redis.exceptions import ConnectionError

import asyncio
from asyncio.exceptions import CancelledError

import sys
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def init_exchanges(redis_client: redis.Redis, exchanges: list):
    """
    Записывает список бирж в Redis при старте программы.
    """
    # Записываем как JSON-строку с TTL=3 дня
    redis_client.set("exchanges", orjson.dumps(exchanges), ex=60*60*24*3)
    print("Список бирж инициализирован в Redis")

def save_prices(data: dict, top_token_list: list):
    pipe = redis_client.pipeline()
    for exchange, symbols in data.items():
        hash_key = f"crypto:{exchange}"
        mapping = {
            symbol: orjson.dumps(fields)
            for symbol, fields in symbols.items()
            if symbol in top_token_list
        }
        if mapping:
            pipe.hset(hash_key, mapping=mapping)
    pipe.execute()

syms = load_tokens_from_file("./data/top_1000_tokens.txt")
top_1000 = [sym + '_USDT' for sym in syms]


bb_unique = ['BOBA_USDT', 'COMBO_USDT', 'DASH_USDT', 'DATA_USDT', 'MVL_USDT', 'PROS_USDT', 'QI_USDT', 'SLF_USDT',
 'PAXG_USDT', 'OSMO_USDT', 'GOMINING_USDT', 'GTC_USDT', 'GEMS_USDT', 'VR_USDT', 'ZEC_USDT', 'ZEN_USDT', 'XVG_USDT',
 'AXL_USDT', 'BEAM_USDT', 'XMR_USDT']
okx_unique = ['KISHU_USDT', 'NFT_USDT', 'AIDOGE_USDT']
gate_unique = ['NPC_USDT', 'SNEK_USDT', 'SWFTC_USDT', 'RACA_USDT', 'ZCX_USDT', 'AMP_USDT', 'FET_USDT', 'QKC_USDT', 'GHST_USDT',
 'FTT_USDT', 'DEGO_USDT', 'D_USDT', 'SYRUP_USDT', 'PEIPEI_USDT', 'DF_USDT', 'DIA_USDT', 'REEF_USDT', 'KEKIUS_USDT', 'WHY_USDT',
 'REI_USDT', 'GT_USDT', 'QUBIC_USDT', 'ADX_USDT', 'SANTOS_USDT', 'BTT_USDT', 'ELON_USDT', 'WEN_USDT', 'OBT_USDT', 'GROK_USDT',
 'MOG_USDT', 'LADYS_USDT', 'LOOM_USDT', 'SUPRA_USDT', 'HSK_USDT', 'AIC_USDT', 'WEMIX_USDT', 'STMX_USDT', 'XEC_USDT', 'ICE_USDT',
 'TOSHI_USDT', 'CHEEMS_USDT', 'PAAL_USDT', 'APU_USDT', 'BLZ_USDT', 'WING_USDT']

# Сохраняем только те токены, которые представлены хотя бы на 2 биржах
unique_tokens = bb_unique + okx_unique + gate_unique
tokens = [token for token in top_1000 if token not in unique_tokens]

print('Активируем базы данных')
manager = ExchangeManager()
manager.add_market("bybit_linear", BybitRestAPI('linear'))
manager.add_market("okx_linear", OKXRestAPI('linear'))
manager.add_market("gate_linear", GateIORestAPI('linear'))

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    exc_list = ['crypto:' + x for x in manager.exchanges.keys()]
    init_exchanges(redis_client, exc_list)
except ConnectionError:
    print('Сервер Redis не отвечает')

print(f'Автоматическое копирование в market_data: {db_manager.get_auto_copy_trigger_state()}')

while True:
    try:
        # start_time = time()
        res_dic = asyncio.run(manager.get_prices())
        # break_1 = time()
        db_manager.update_data(res_dic, tokens)
        # break_2 = time()
        # save_prices(res_dic, top_500)
        # break_3 = time()
        # print('request time:', break_1 - start_time)
        # print('Postgre update time:', break_2 - break_1)
        # print('Redis update time:', break_3 - break_2)
        # print('overall time:', break_3 - start_time)
        # break
        sleep(0.1)
    except (KeyboardInterrupt, CancelledError):
        print('Завершение работы.')
        break
