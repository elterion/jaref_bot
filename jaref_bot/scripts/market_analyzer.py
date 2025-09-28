from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI
from jaref_bot.utils.coins import get_step_info, get_price_scale
from jaref_bot.trading.functions import set_leverage

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name

import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
from time import sleep
import pickle
import ast
import json

from zoneinfo import ZoneInfo
from datetime import timedelta
from jaref_bot.utils.pair_trading import get_lr_zscore
from uuid import uuid4
import math
from functools import lru_cache

def round_down(value: float, dp: float):
    return round(math.floor(value / dp) * dp, 6)

def get_thresholds():
    data = []
    with open('d:/python/crypto/jaref_bot/config/thresholds.txt', 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                tuple_data = ast.literal_eval(line)
                data.append(tuple_data)
    return data

def place_order(token_1, token_2, pos_side, qty_1, qty_2, t1_price, t2_price,
                t1_orig_side, t2_orig_side, ts, dp_1, dp_2, db_manager, redis_manager):
    side_1 = 'Buy' if t1_orig_side == 'long' else 'Sell'
    side_2 = 'Buy' if t1_orig_side == 'short' else 'Sell'

    if pos_side == 'open':
        db_manager.add_pair_order(token_1=token_1, token_2=token_2, side=t1_orig_side, qty_1=qty_1, qty_2=qty_2)

        redis_manager.add_order(exchange='bybit',
                                token=token_1,
                                qty=qty_1,
                                side=side_1,
                                action='open',
                                price=t1_price,
                                leverage=leverage,
                                dp=dp_1,
                                ord_id=str(uuid4()),
                                ts=ts,
                                status='created')
        redis_manager.add_order(exchange='bybit',
                                token=token_2,
                                qty=qty_2,
                                side=side_2,
                                action='open',
                                price=t2_price,
                                leverage=leverage,
                                dp=dp_2,
                                ord_id=str(uuid4()),
                                ts=ts,
                                status='created')

    elif pos_side == 'close':
        db_manager.close_pair_order(token_1=token_1, token_2=token_2, side=t1_orig_side)

        redis_manager.add_order(exchange='bybit',
                                token=token_1,
                                qty=qty_1,
                                side=side_2,
                                action='close',
                                price=t1_price,
                                leverage=leverage,
                                dp=dp_1,
                                ord_id=str(uuid4()),
                                ts=ts,
                                status='created')
        redis_manager.add_order(exchange='bybit',
                                token=token_2,
                                qty=qty_2,
                                side=side_1,
                                action='close',
                                price=t2_price,
                                leverage=leverage,
                                dp=dp_2,
                                ord_id=str(uuid4()),
                                ts=ts,
                                status='created')
    else:
        raise Exception('Параметр pos_side может принимать значения только "open" или "close"')

def check_open_conditions(token: str,
                          current_orders: pd.DataFrame,
                          pending_orders: dict,
                          min_order: float,
                          max_position: float) -> float:
    """
        Функция проверяет условия для открытия нового ордера. Новый ордер будет открыт если:
        - Токена ещё нет ни в текущих ордерах, ни в открытых позициях
        - Токен есть в открытых позициях, но размер позиции позволяет открыть ещё одну сделку.

        Args:
            token - название токена. Например, 'ADA_USDT'.
            current_orders - датафрейм с текущими позициями.
            pending_orders - словарь с текущими ордерами.
            max_position - максимально возможный размер позиции на бирже.
        Return:
            Размер ордера в usdt, который можно открыть.

    """
    # Сначала проверяем условие, что такой ордер ещё не открывали
    token_in_positions = token in current_orders['token'].to_list()

    token_in_pending = False
    position_size = 0
    for exc, data in pending_orders.items():
        for tok in data:
            if tok == token:
                token_in_pending = True
                position_size += float(data[tok]['qty'])
    if not (token_in_positions or token_in_pending):
        return max_position

    # Проверяем условие, что ордер уже есть среди открытых позиций, но размер позиции позволяет добавить
    # if token_in_positions:
    #     try:
    #         price = current_orders.filter(pl.col('token') == token)['price'].max()
    #         opened_position = current_orders.filter(pl.col('token') == token)['usdt_amount'].max()

    #         position_usdt = position_size * price + opened_position
    #         if position_usdt < max_position - min_order:
    #             return max_position - position_usdt
    #     except TypeError: # Ситуация, когда команда на закрытие позиции по этому токену уже отдана, но сам токен из БД ещё не убран
    #         return False


    return False

def change_position(token_1, token_2, pos_side, t1_orig_side, t2_orig_side, t1_data, t2_data, t1_usdt_amount, t2_usdt_amount,
                    leverage, min_order, fee_rate, ts, coin_information, db_manager, redis_manager, log_data):
    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    t1_min_qty_step = get_step_info(coin_information, token_1, 'bybit_linear', 'bybit_linear')
    t2_min_qty_step = get_step_info(coin_information, token_2, 'bybit_linear', 'bybit_linear')

    dp_1 = coin_information['bybit_linear'][token_1]['price_scale']
    dp_2 = coin_information['bybit_linear'][token_2]['price_scale']

    if pos_side == 'open':
        t1_price = t1_data['askprice_0'][0] if t1_orig_side == 'long' else t1_data['bidprice_0'][0]
        t2_price = t2_data['bidprice_0'][0] if t1_orig_side == 'long' else t2_data['askprice_0'][0]
        t1_vol = t1_data['askvolume_0'][0] if t1_orig_side == 'long' else t1_data['bidvolume_0'][0]
        t2_vol = t2_data['bidvolume_0'][0] if t1_orig_side == 'long' else t2_data['askvolume_0'][0]

        t1_avail_usdt = t1_vol / t1_price
        t2_avail_usdt = t2_vol / t2_price

        avail_usdt = min(t1_avail_usdt, t2_avail_usdt, t1_usdt_amount, t2_usdt_amount) * leverage

        if avail_usdt > min_order:
            qty_1 = round_down(avail_usdt / (1.0 + 2.0 * fee_rate) / t1_price, t1_min_qty_step)
            qty_2 = round_down(avail_usdt / (1.0 + 2.0 * fee_rate) / t2_price, t2_min_qty_step)

            place_order(token_1=token_1, token_2=token_2, pos_side=pos_side, qty_1=qty_1, qty_2=qty_2,
                t1_price=t1_price, t2_price=t2_price, t1_orig_side=t1_orig_side, t2_orig_side=t2_orig_side,
                ts=ts, dp_1=dp_1, dp_2=dp_2, db_manager=db_manager, redis_manager=redis_manager)

            act_1 = 'Buy' if t1_orig_side == 'long' else 'Sell'
            act_2 = 'Sell' if t1_orig_side == 'long' else 'Buy'
            print(f'{ct} [{t1_orig_side.title()} open] {act_1} {qty_1} {token_1[:-5]} for {t1_price}; {act_2} {qty_2} {token_2[:-5]} for {t2_price}; z_score: {log_data['z_score']:.2f}')

            write_order_log(ts, ct, token_1, token_2, log_data['tf'], log_data['wind'],
                log_data['thresh_in'], log_data['thresh_out'], t1_orig_side, 'open',
                log_data['t1'], log_data['t2'],
                t1_data['bidprice_0'][0], t1_data['askprice_0'][0], t2_data['bidprice_0'][0],
                t2_data['askprice_0'][0], t1_data['bidvolume_0'][0], t1_data['askvolume_0'][0],
                t2_data['bidvolume_0'][0], t2_data['askvolume_0'][0], qty_1, qty_2,
                log_data['z_score'], log_data['beta'])


    elif pos_side == 'close':
        t1_price = t1_data['bidprice_0'][0] if t1_orig_side == 'long' else t1_data['askprice_0'][0]
        t2_price = t2_data['askprice_0'][0] if t1_orig_side == 'long' else t2_data['bidprice_0'][0]
        t1_vol = t1_data['bidvolume_0'][0] if t1_orig_side == 'long' else t1_data['askvolume_0'][0]
        t2_vol = t2_data['askvolume_0'][0] if t1_orig_side == 'long' else t2_data['bidvolume_0'][0]

        current_orders = db_manager.get_table('current_orders', df_type='polars')
        first_leg = current_orders.filter(pl.col('token') == token_1)
        second_leg = current_orders.filter(pl.col('token') == token_2)

        open_qty_1 = first_leg['qty'][0]
        open_qty_2 = second_leg['qty'][0]

        t1_avail_volume = t1_price * t1_vol
        t2_avail_volume = t2_price * t2_vol
        # avail_exit_usdt = min(t1_avail_volume, t2_avail_volume) / leverage

        if t1_vol > open_qty_1 and t2_vol > open_qty_2:
            place_order(token_1, token_2, pos_side, open_qty_1, open_qty_2, t1_price, t2_price,
                t1_orig_side, t2_orig_side, ts, dp_1, dp_2, db_manager, redis_manager)

            act_1 = 'Sell' if t1_orig_side == 'long' else 'Buy'
            act_2 = 'Buy' if t1_orig_side == 'long' else 'Sell'
            print(f'{ct} [{t1_orig_side.title()} closed] {act_1} {open_qty_1} {token_1[:-5]} for {t1_price}; {act_2} {open_qty_2} {token_2[:-5]} for {t2_price}; z_score: {log_data['z_score']:.2f}')

            write_order_log(ts, ct, token_1, token_2, log_data['tf'], log_data['wind'],
                log_data['thresh_in'], log_data['thresh_out'], t1_orig_side, 'close',
                log_data['t1'], log_data['t2'],
                t1_data['bidprice_0'][0], t1_data['askprice_0'][0], t2_data['bidprice_0'][0],
                t2_data['askprice_0'][0], t1_data['bidvolume_0'][0], t1_data['askvolume_0'][0],
                t2_data['bidvolume_0'][0], t2_data['askvolume_0'][0], open_qty_1, open_qty_2,
                log_data['z_score'], log_data['beta'])

def get_hist_df(postgre_manager, start_time):
    hour_1_df = postgre_manager.get_orderbooks(interval='1h', start_date=start_time)
    hour_1_df = hour_1_df.with_columns(pl.col('price').alias('avg_price'))

    hour_4_df = postgre_manager.get_orderbooks(interval='4h', start_date=start_time)
    hour_4_df = hour_4_df.with_columns(pl.col('price').alias('avg_price'))

    return hour_4_df, hour_1_df

def calculate_profit(open_price, close_price, n_coins, side, fee_rate=0.00055):
    usdt_open = n_coins * open_price
    open_fee = usdt_open * fee_rate

    # print(f'{n_coins=}; {close_price=}')

    usdt_close = n_coins * close_price
    close_fee = usdt_close * fee_rate

    if side == 'long':
        profit = usdt_close - usdt_open - open_fee - close_fee
    elif side == 'short':
        profit = usdt_open - usdt_close - open_fee - close_fee
    return profit

@lru_cache
def set_leverage_cached(token, leverage):
    set_leverage(demo=demo, exc='bybit_linear', symbol=token + '_USDT', leverage=leverage)

def write_order_log(ts, ct, token_1, token_2, tf, wind, thresh_in, thresh_out, side, action,
                    t1, t2, t1_bid_price, t1_ask_price, t2_bid_price, t2_ask_price,
                    t1_bid_size, t1_ask_size, t2_bid_size, t2_ask_size, qty_1, qty_2,
                    z_score, beta=None):
    """
    Запись сделки в лог файл.
    ts - unix timestamp
    ct - текущее время в формате datetime
    token_1 - название токена_1
    token_2 - название токена_2
    tf - таймфрейм
    wind - размер скользящего окна
    t1 - исторические данные для токена_1
    t2 - исторические данные для токена_2
    t1_df_sec - датафрейм с последними записями токена_1
    t2_df_sec - датафрейм с последними записями токена_1
    z_score - z_score

    На выходе ключи словаря t1_last и t2_last являются последними ценами
    токенов, взятыми из t1_df_sec и t2_df_sec.

    """


    t1 = [round(x, 6) for x in t1.tolist()]
    t2 = [round(x, 6) for x in t2.tolist()]
    z_score = round(float(z_score), 2)
    beta = round(float(beta), 2)

    log = {'ts': ts,
            'ct': ct,
            'token_1': token_1[:-5],
            'token_2': token_2[:-5],
            'tf': tf,
            'wind': wind,
            'thresh_in': thresh_in,
            'thresh_out': thresh_out,
            'side': side,
            'action': action,
            't1': t1,
            't2': t2,
            't1_bid_price': t1_bid_price,
            't1_ask_price': t1_ask_price,
            't2_bid_price': t2_bid_price,
            't2_ask_price': t2_ask_price,
            't1_bid_size': t1_bid_size,
            't1_ask_size': t1_ask_size,
            't2_bid_size': t2_bid_size,
            't2_ask_size': t2_ask_size,
            'qty_1': qty_1,
            'qty_2': qty_2,
            'z_score': z_score,
            'beta': beta
           }
    json_log = json.dumps(log, default=float, ensure_ascii=False)

    with open('./logs/trades.jsonl', 'a', encoding='utf-8') as f:
        f.write(json_log + '\n')

def main(demo, open_new_orders, tf, wind, thresh_in, thresh_out,
         max_position, min_order, max_pairs, leverage, fee_rate, td):
    update_positions_flag = False

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Начинаем работу...')

    # --- Загружаем все коинтегрированные токены ---
    cointegrated_tokens = []
    with open('./jaref_bot/config/cointegrated_tokens.txt', 'r') as file:
        for line in file:
            a, b = line.strip().split()
            cointegrated_tokens.append((a, b))

    # --- Загружаем техническую информацию по монетам с биржи ---
    exc_manager = ExchangeManager()
    exc_manager.add_market("bybit_linear", BybitRestAPI('linear'))
    coin_information = exc_manager.get_instrument_data()

    with open("./data/coin_information.pkl", "wb") as f:
        pickle.dump(coin_information, f)

    # --- Инициируем менеджеры, работающие с БД ---
    db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
    postgre_manager = DBManager(db_params)
    redis_orders = RedisManager(db_name = 'orders')
    redis_orderbooks = RedisManager(db_name = 'orderbooks')
    redis_sys = RedisManager(db_name = 'system_state')

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Обновление плечей на бирже ByBit')
    for t1_name, t2_name in cointegrated_tokens:
        set_leverage_cached(token=t1_name, leverage=leverage)
        set_leverage_cached(token=t2_name, leverage=leverage)

    low_in = -thresh_in
    low_out = -thresh_out
    high_in = thresh_in
    high_out = thresh_out

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Старт основного цикла.')
    while True:
        try:
            zscore_arr = []

            time_now = datetime.now()
            ts = int(datetime.timestamp(time_now))
            ct = time_now.strftime('%Y-%m-%d %H:%M:%S')
            print(f'Контрольное время: {ct}', end='\r')

            # --- Устанавливаем heartbeat отметку в Redis ---
            redis_sys.set_system_state('market_analyzer', 1)

            # --- Проверяем работу модуля trades_executor ---
            if not redis_sys.get_system_state('trades_executor'):
                print(f'{ct} Потеряна связь с trades_executor!')
                break

            # --- Подгружаем исторические датафреймы 1 раз в час ---
            end_time = datetime.now().replace(tzinfo=ZoneInfo("Europe/Moscow"))
            start_time = end_time - timedelta(hours = td)

            try:
                last_updates_1h = (datetime.now(ZoneInfo("Europe/Moscow")) - hour_1_df[-1]['time'][0]).seconds
            except NameError:
                hour_4_df, hour_1_df = get_hist_df(postgre_manager, start_time)
                last_updates_1h = (datetime.now(ZoneInfo("Europe/Moscow")) - hour_1_df[-1]['time'][0]).seconds

            if last_updates_1h > 3665: # 1 час 1 минута 5 секунд
                hour_4_df, hour_1_df = get_hist_df(postgre_manager, start_time)

            # --- Текущие данные ---
            current_data = redis_orderbooks.get_orderbooks(1)
            if current_data.is_empty():
                print(f'{ct} current data is empty!')
                sleep(10)
                continue

            current_data = current_data.with_columns(
                    ((pl.col('bidprice_0') + pl.col('askprice_0')) / 2.0).alias('avg_price')
                ).drop('exchange', 'market_type')

            pending_orders = redis_orders.get_pending_orders()
            current_orders = postgre_manager.get_table('current_orders', df_type='polars')
            pairs = postgre_manager.get_table('pairs', df_type='polars')

            # --- Секундный датафрейм для подсчёта среднего значения ---
            end_t = datetime.now().replace(tzinfo=ZoneInfo("Europe/Moscow"))
            st_t = end_t - timedelta(seconds = 20)

            tick_df = postgre_manager.get_tick_ob(start_time=st_t).with_columns(
                ((pl.col('bid_price') + pl.col('ask_price')) / 2.0).alias('avg_price')
            ).filter(
                (pl.col('bid_size') * pl.col('bid_price') > min_order) &
                (pl.col('ask_size') * pl.col('ask_price') > min_order)
            )

            # --- Обрабатываем каждую пару токенов ---
            for t1_name, t2_name in cointegrated_tokens:
                token_1 = t1_name + '_USDT'
                token_2 = t2_name + '_USDT'
                z_score = 0

                # --- Обновляем открытые пары и текущие ордеры ---
                if update_positions_flag:
                    pending_orders = redis_orders.get_pending_orders()
                    current_orders = postgre_manager.get_table('current_orders', df_type='polars')
                    pairs = postgre_manager.get_table('pairs', df_type='polars')
                    update_positions_flag = False

                # --- Выбираем из общего датафрейма нужные токены ---
                t1_tick_df = tick_df.filter(pl.col('token') == token_1)
                t2_tick_df = tick_df.filter(pl.col('token') == token_2)

                # --- Проверяем, что датафрейм не пустой ---
                if t1_tick_df.height < 2 or t2_tick_df.height < 2:
                    continue

                # --- Проверяем, что этой пары нет в очереди на закрытие ---
                if pairs.filter((pl.col('token_1') == token_1) &
                                (pl.col('token_2') == token_2) &
                                (pl.col('status') == 'closing')).height > 0:
                    continue

                # Пропускаем пару, если уже открыто максимальное количество позиций,
                # а для этой пары позиция не открыта
                if (pairs.height >= max_pairs and pairs.filter(
                            (pl.col('token_1') == token_1) & (pl.col('token_2') == token_2)
                            ).is_empty()
                    ):
                    continue

                # --- Получаем средние цены за исторический период ---
                hist_df = hour_1_df if tf == '1h' else hour_4_df

                token_1_hist_price = hist_df.filter(pl.col('token') == token_1).tail(2 * wind + 1)['avg_price'].to_numpy()
                token_2_hist_price = hist_df.filter(pl.col('token') == token_2).tail(2 * wind + 1)['avg_price'].to_numpy()

                # --- Получаем текущие цены ---
                t1_curr_data = current_data.filter(pl.col('symbol') == token_1)
                t2_curr_data = current_data.filter(pl.col('symbol') == token_2)

                # --- Проверка на актуальность текущих цен ---
                try:
                    t1_ts = t1_curr_data['ts'].item()
                    t2_ts = t2_curr_data['ts'].item()
                except ValueError: # Ситуация, когда после восстановления соединения не все токены успевают обновиться
                    sleep(1)
                    break

                if abs(t1_ts - t2_ts) > 10: # Если разница во времени между двумя ценами больше 10 секунд, пропускаем эту пару
                    continue



                t1_med = np.append(token_1_hist_price, t1_tick_df['avg_price'].median())
                t2_med = np.append(token_2_hist_price, t2_tick_df['avg_price'].median())
                t1_curr = np.append(token_1_hist_price, t1_curr_data['avg_price'][0])
                t2_curr = np.append(token_2_hist_price, t2_curr_data['avg_price'][0])

                _, _, _, _, beta, zscore = get_lr_zscore(t1_med, t2_med, np.array([wind]))
                _, _, _, _, beta_curr, zscore_curr = get_lr_zscore(t1_curr, t2_curr, np.array([wind]))
                z_score = zscore[0]
                z_score_curr = zscore_curr[0]
                beta = beta[0]

                # ----- Проверяем условия для входа в позицию -----
                if open_new_orders and pairs.height < max_pairs:
                    try:
                        t1_usdt_amount = check_open_conditions(token_1, current_orders, pending_orders, min_order, max_position)
                        t2_usdt_amount = check_open_conditions(token_2, current_orders, pending_orders, min_order, max_position)
                    except KeyError: # Данные в pending_orders не успели обновиться
                        continue

                    if t1_usdt_amount and t2_usdt_amount:
                        # Проверяем открытие long-позиции по token_1 и short-позиции по token_2
                        if zscore < low_in and z_score_curr < low_in:
                            change_position(token_1, token_2, pos_side='open', t1_orig_side='long', t2_orig_side='short',
                                t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=t1_usdt_amount,
                                t2_usdt_amount=t2_usdt_amount, leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                                coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders,
                                log_data={'tf': tf, 'wind': wind, 'thresh_in': thresh_in, 'thresh_out': thresh_out,
                                          't1': t1_med, 't2': t2_med, 'beta': beta, 'z_score': z_score})
                            update_positions_flag = True
                            break

                        # Проверяем открытие short-позиции по token_1 и long-позиции по token_2
                        if zscore > high_in and z_score_curr > high_in:
                            change_position(token_1, token_2, pos_side='open', t1_orig_side='short', t2_orig_side='long',
                                t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=t1_usdt_amount,
                                t2_usdt_amount=t2_usdt_amount, leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                                coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders,
                                log_data={'tf': tf, 'wind': wind, 'thresh_in': thresh_in, 'thresh_out': thresh_out,
                                          't1': t1_med, 't2': t2_med, 'beta': beta, 'z_score': z_score})
                            update_positions_flag = True
                            break

                # ----- Проверяем условия выхода из позиции -----
                first_leg = current_orders.filter(pl.col('token') == token_1)
                second_leg = current_orders.filter(pl.col('token') == token_2)

                if first_leg.is_empty() or second_leg.is_empty(): # Если данные не успели обновиться
                    continue

                long_opened =  pairs.filter(
                        (pl.col('token_1') == token_1) & (pl.col('token_2') == token_2) & (pl.col('side') == 'long')
                    ).height == 1
                short_opened =  pairs.filter(
                        (pl.col('token_1') == token_1) & (pl.col('token_2') == token_2) & (pl.col('side') == 'short')
                    ).height == 1

                # --- Добавляем текущий z_score и profit в таблицу БД ---
                if long_opened or short_opened:
                    t1_op = first_leg['price'][0]
                    t2_op = second_leg['price'][0]
                    q1 = first_leg['qty'][0]
                    q2 = second_leg['qty'][0]

                    side_1 = 'long' if long_opened else 'short'
                    side_2 = 'short' if long_opened else 'long'

                    curr_profit_1 = calculate_profit(open_price=t1_op, close_price=t1_tick_df['avg_price'].median(), n_coins=q1, side=side_1)
                    curr_profit_2 = calculate_profit(open_price=t2_op, close_price=t2_tick_df['avg_price'].median(), n_coins=q2, side=side_2)

                    curr_profit = curr_profit_1 + curr_profit_2
                    zscore_arr.append((ts, 'bybit', token_1, token_2, curr_profit, z_score))

                # --- Выходим из позиции, если позволяют условия ---
                if long_opened and zscore > high_out and z_score_curr > high_out:
                    change_position(token_1, token_2, pos_side='close', t1_orig_side='long', t2_orig_side='short',
                        t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=None, t2_usdt_amount=None,
                        leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                        coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders,
                                log_data={'tf': tf, 'wind': wind, 'thresh_in': thresh_in, 'thresh_out': thresh_out,
                                          't1': t1_med, 't2': t2_med, 'beta': beta, 'z_score': z_score})
                    update_positions_flag = True
                    break

                if short_opened and zscore < low_out and z_score_curr < low_out:
                    change_position(token_1, token_2, pos_side='close', t1_orig_side='short', t2_orig_side='long',
                        t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=None, t2_usdt_amount=None,
                        leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                        coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders,
                                log_data={'tf': tf, 'wind': wind, 'thresh_in': thresh_in, 'thresh_out': thresh_out,
                                          't1': t1_med, 't2': t2_med, 'beta': beta, 'z_score': z_score})
                    update_positions_flag = True
                    break

            try:
                postgre_manager.add_data_to_zscore_history(zscore_arr)
            except KeyboardInterrupt:
                # print(zscore_arr)
                # print(err)
                break
            sleep(0.5)

        except KeyboardInterrupt:
            print('\nЗавершение работы.')
            break


if __name__ == '__main__':
    demo = True
    open_new_orders = True # Открывать новые позиции или только закрываем уже существующие


    exchange = 'bybit'
    min_order = 40     # Минимальный размер ордера
    max_position = 50  # Максимальный размер одного плеча в парной позиции
    max_pairs = 5      # Максимальное кол-во открытых позиций
    leverage = 2       # Плечо
    fee_rate = 0.00055 # Процент комиссии биржи
    td = 120            # За сколько последних часов брать историю

    tf = '4h'
    wind = 8
    thresh_in = 1.8
    thresh_out = 0.25


    main(demo, open_new_orders, tf, wind, thresh_in, thresh_out,
         max_position, min_order, max_pairs, leverage, fee_rate, td)
