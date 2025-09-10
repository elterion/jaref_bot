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
from time import sleep, time
from decimal import Decimal, ROUND_DOWN
import pickle
import ast

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
    if pos_side == 'open':
        db_manager.add_pair_order(token_1=token_1, token_2=token_2, side=t1_orig_side, qty_1=qty_1, qty_2=qty_2)
        redis_manager.add_order(exchange='bybit', token=token_1, qty=qty_1, side=t1_orig_side, price=t1_price,
                       leverage=leverage, dp=dp_1, ord_id=str(uuid4()), ts=ts, status='created')
        redis_manager.add_order(exchange='bybit', token=token_2, qty=qty_2, side=t2_orig_side, price=t2_price,
                       leverage=leverage, dp=dp_2, ord_id=str(uuid4()), ts=ts, status='created')

    elif pos_side == 'close':
        db_manager.delete_pair_order(token_1=token_1, token_2=token_2)
        redis_manager.add_order(exchange='bybit', token=token_1, qty=qty_1, side=t2_orig_side, price=t1_price,
                       leverage=leverage, dp=dp_1, ord_id=str(uuid4()), ts=ts, status='created')
        redis_manager.add_order(exchange='bybit', token=token_2, qty=qty_2, side=t1_orig_side, price=t2_price,
                       leverage=leverage, dp=dp_2, ord_id=str(uuid4()), ts=ts, status='created')
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
    if token_in_positions:
        try:
            price = current_orders.filter(pl.col('token') == token)['price'].max()
            opened_position = current_orders.filter(pl.col('token') == token)['usdt_amount'].max()

            position_usdt = position_size * price + opened_position
            if position_usdt < max_position - min_order:
                return max_position - position_usdt
        except TypeError: # Ситуация, когда команда на закрытие позиции по этому токену уже отдана, но сам токен из БД ещё не убран
            return False


    return False

def change_position(token_1, token_2, pos_side, t1_orig_side, t2_orig_side, t1_data, t2_data, t1_usdt_amount, t2_usdt_amount,
                    leverage, min_order, fee_rate, ts, z_score, coin_information, db_manager, redis_manager):
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
            print(f'{ct} [{t1_orig_side.title()} open] {act_1} {qty_1} {token_1[:-5]} for {t1_price}; {act_2} {qty_2} {token_2[:-5]} for {t2_price}; z_score: {z_score:.2f}')

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
            # debug_arr.append({'token_1': token_1, 'token_2': token_2, 't1': t1, 't2': t2,
            #                   't1_med_price': t1_med_price, 't2_med_price': t2_med_price, 'zscore': zscore})

            db_manager.delete_pair_order(token_1, token_2)
            redis_manager.add_order(exchange='bybit', token=token_1, qty=open_qty_1, side=t2_orig_side,
                                   leverage=leverage, dp=dp_1, ord_id=str(uuid4()), ts=ts, price=t1_price, status='created')
            redis_manager.add_order(exchange='bybit', token=token_2, qty=open_qty_2, price=t2_price, side=t1_orig_side,
                                   leverage=leverage, dp=dp_2, ord_id=str(uuid4()), ts=ts, status='created')

            act_1 = 'Sell' if t1_orig_side == 'long' else 'Buy'
            act_2 = 'Buy' if t1_orig_side == 'long' else 'Sell'
            print(f'{ct} [{t1_orig_side.title()} closed] {act_1} {open_qty_1} {token_1[:-5]} for {t1_price}; {act_2} {open_qty_2} {token_2[:-5]} for {t2_price}; z_score: {z_score:.2f}')

def get_hist_df(postgre_manager, start_time):
    hour_1_df = postgre_manager.get_orderbooks(exchange='bybit',
                                             market_type='linear',
                                             interval='1h',
                                             start_date=start_time)
    cols_to_drop = ('exchange', 'market_type', 'bid_price', 'bid_size', 'ask_price', 'ask_size')
    hour_1_df = hour_1_df.with_columns(
            ((pl.col('bid_price') + pl.col('ask_price')) / 2.0).alias('avg_price')
        )
    hour_1_df = hour_1_df.drop(cols_to_drop)

    hour_4_df = postgre_manager.get_orderbooks(exchange='bybit',
                                         market_type='linear',
                                         interval='4h',
                                         start_date=start_time)
    hour_4_df = hour_4_df.with_columns(
            ((pl.col('bid_price') + pl.col('ask_price')) / 2.0).alias('avg_price')
        )
    hour_4_df = hour_4_df.drop(cols_to_drop)

    return hour_4_df, hour_1_df

@lru_cache
def set_leverage_cached(token, leverage):
    set_leverage(demo=demo, exc='bybit_linear', symbol=token + '_USDT', leverage=leverage)


def main(demo, open_new_orders, max_position, min_order, max_pairs, leverage, fee_rate, td):
    update_positions_flag = False

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Начинаем работу...')

    params = get_thresholds()
    token_params = sorted(params, key=lambda x: x[0], reverse=True)

    exc_manager = ExchangeManager()
    exc_manager.add_market("bybit_linear", BybitRestAPI('linear'))

    db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
    postgre_manager = DBManager(db_params)
    redis_orders = RedisManager(db_name = 'orders')
    redis_orderbooks = RedisManager(db_name = 'orderbooks')

    coin_information = exc_manager.get_instrument_data()

    with open("./data/coin_information.pkl", "wb") as f:
        pickle.dump(coin_information, f)

    end_time = datetime.now().replace(tzinfo=ZoneInfo("Europe/Moscow"))
    start_time = end_time - timedelta(hours = td)

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Обновление плечей на бирже ByBit')
    for _, t1_name, t2_name, tf, wind, thresh_in, thresh_out in token_params:
        set_leverage_cached(token=t1_name, leverage=leverage)
        set_leverage_cached(token=t2_name, leverage=leverage)

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Старт основного цикла.')
    while True:
        try:
            time_now = datetime.now()
            ts = int(datetime.timestamp(time_now))
            ct = time_now.strftime('%Y-%m-%d %H:%M:%S')

            # --- Подгружаем исторические датафреймы 1 раз в час ---
            try:
                last_updates_1h = (datetime.now(ZoneInfo("Europe/Moscow")) - hour_1_df[-1]['bucket'][0]).seconds
            except NameError:
                hour_4_df, hour_1_df = get_hist_df(postgre_manager, start_time)
                last_updates_1h = (datetime.now(ZoneInfo("Europe/Moscow")) - hour_1_df[-1]['bucket'][0]).seconds

            if last_updates_1h > 3665: # 1 час 1 минута 5 секунд
                hour_4_df, hour_1_df = get_hist_df(postgre_manager, start_time)

            # --- Текущие данные ---
            current_data = redis_orderbooks.get_orderbooks(1)
            if current_data.is_empty():
                print(f'{ct} current data is empty!')
                sleep(5)
                continue

            current_data = current_data.with_columns(
                    ((pl.col('bidprice_0') + pl.col('askprice_0')) / 2.0).alias('avg_price')
                ).drop('exchange', 'market_type')

            pending_orders = redis_orders.get_pending_orders()
            current_orders = postgre_manager.get_table('current_orders', df_type='polars')
            pairs = postgre_manager.get_table('pairs', df_type='polars')

            # --- Секундный датафрейм для подсчёта среднего значения ---
            end_t = datetime.now().replace(tzinfo=ZoneInfo("Europe/Moscow"))
            st_t = end_t - timedelta(seconds = 6)

            df_sec = postgre_manager.get_raw_orderbooks(exchange='bybit', market_type='linear', start_time=st_t).with_columns(
                ((pl.col('bid_price') + pl.col('ask_price')) / 2.0).alias('avg_price')
            ).filter(
                (pl.col('bid_size') * pl.col('bid_price') > min_order) &
                (pl.col('ask_size') * pl.col('ask_price') > min_order)
            )

            # --- Обрабатываем каждую пару токенов ---
            for _, t1_name, t2_name, tf, wind, thresh_in, thresh_out in token_params:
                token_1 = t1_name + '_USDT'
                token_2 = t2_name + '_USDT'

                low_in = -1.8
                low_out = -0.5
                high_in = 1.8
                high_out = 0.5

                # --- Получаем информацию о кол-ве знаков после запятой в цене токена для округления цен ---
                dp_1 = coin_information['bybit_linear'][token_1]['price_scale']
                dp_2 = coin_information['bybit_linear'][token_2]['price_scale']

                # --- Обновляем открытые пары и текущие ордеры
                if update_positions_flag:
                    pending_orders = redis_orders.get_pending_orders()
                    current_orders = postgre_manager.get_table('current_orders', df_type='polars')
                    pairs = postgre_manager.get_table('pairs', df_type='polars')
                    update_positions_flag = False

                # --- Получаем средние цены за исторический период ---
                hist_df = hour_1_df if tf == '1h' else hour_4_df

                # Убираем последнее значение, которое постоянно обновляется
                token_1_hist_price = hist_df.filter(pl.col('token') == token_1).tail(2 * wind + 1)['avg_price'][:-1]
                token_2_hist_price = hist_df.filter(pl.col('token') == token_2).tail(2 * wind + 1)['avg_price'][:-1]

                # --- Получаем текущие цены ---
                t1_curr_data = current_data.filter(pl.col('symbol') == token_1)
                t2_curr_data = current_data.filter(pl.col('symbol') == token_2)

                # --- Проверка на актуальность текущих цен ---
                t1_ts = t1_curr_data['ts'].item()
                t2_ts = t2_curr_data['ts'].item()

                if abs(t1_ts - t2_ts) > 5: # Если разница во времени между двумя ценами больше 5 секунд, пропускаем эту пару
                    continue

                # Вместо текущей цены подставляем в функцию для расчёта z_score медианную цену за 5 секунд, чтобы избежать выбросов
                t1_df_sec = df_sec.filter(pl.col('token') == token_1)
                t2_df_sec = df_sec.filter(pl.col('token') == token_2)

                if t1_df_sec.height >= 3 and t2_df_sec.height >= 3:
                    t1_med_price = t1_df_sec['avg_price'].median()
                    t2_med_price = t2_df_sec['avg_price'].median()
                else:
                    continue

                t1 = token_1_hist_price.append(pl.Series("avg_price", [t1_med_price])).to_numpy()
                t2 = token_2_hist_price.append(pl.Series("avg_price", [t2_med_price])).to_numpy()

                alpha, beta, zscore = get_lr_zscore(t1, t2, np.array([wind]))
                z_score = zscore[0]

                # ----- Проверяем условия для входа в позицию -----
                if open_new_orders and pairs.height < max_pairs:
                    try:
                        t1_usdt_amount = check_open_conditions(token_1, current_orders, pending_orders, min_order, max_position)
                        t2_usdt_amount = check_open_conditions(token_2, current_orders, pending_orders, min_order, max_position)
                    except KeyError: # Данные в pending_orders не успели обновиться
                        continue

                    if t1_usdt_amount and t2_usdt_amount:
                        # Проверяем открытие long-позиции по token_1 и short-позиции по token_2
                        if zscore < low_in:
                            change_position(token_1, token_2, pos_side='open', t1_orig_side='long', t2_orig_side='short',
                                t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=t1_usdt_amount,
                                t2_usdt_amount=t2_usdt_amount, leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                                z_score=z_score, coin_information=coin_information,
                                db_manager=postgre_manager, redis_manager=redis_orders)
                            update_positions_flag = True
                            break

                        # Проверяем открытие short-позиции по token_1 и long-позиции по token_2
                        if zscore > high_in:
                            change_position(token_1, token_2, pos_side='open', t1_orig_side='short', t2_orig_side='long',
                                t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=t1_usdt_amount,
                                t2_usdt_amount=t2_usdt_amount, leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts,
                                z_score=z_score, coin_information=coin_information,
                                db_manager=postgre_manager, redis_manager=redis_orders)
                            update_positions_flag = True
                            break

                # ----- Проверяем условия выхода из позиции -----
                first_leg = current_orders.filter(pl.col('token') == token_1)
                second_leg = current_orders.filter(pl.col('token') == token_2)

                if first_leg.is_empty() or second_leg.is_empty(): # Если данные не успели обновиться
                    continue

                long_opened =  pairs.filter((pl.col('token_1') == token_1) & (pl.col('token_2') == token_2) & (pl.col('side') == 'long')).height == 1
                short_opened =  pairs.filter((pl.col('token_1') == token_1) & (pl.col('token_2') == token_2) & (pl.col('side') == 'short')).height == 1

                if zscore > high_out and long_opened:
                    change_position(token_1, token_2, pos_side='close', t1_orig_side='long', t2_orig_side='short',
                        t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=None, t2_usdt_amount=None,
                        leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts, z_score=z_score,
                        coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders)
                    update_positions_flag = True
                    break

                if zscore < low_out and short_opened:
                    change_position(token_1, token_2, pos_side='close', t1_orig_side='short', t2_orig_side='long',
                        t1_data=t1_curr_data, t2_data=t2_curr_data, t1_usdt_amount=None, t2_usdt_amount=None,
                        leverage=leverage, min_order=min_order, fee_rate=fee_rate, ts=ts, z_score=z_score,
                        coin_information=coin_information, db_manager=postgre_manager, redis_manager=redis_orders)
                    update_positions_flag = True
                    break

            sleep(0.5)

        except KeyboardInterrupt:
            print('Завершение работы.')
            break


if __name__ == '__main__':
    demo=True
    exchange = 'bybit'
    min_order = 50     # Минимальный размер ордера
    max_position = 100 # Максимальный размер одного плеча в парной позиции
    max_pairs = 5      # Максимальное кол-во открытых позиций
    leverage = 2       # Плечо
    fee_rate = 0.00055 # Процент комиссии биржи
    td = 120           # За сколько последних часов брать историю

    open_new_orders = True # Открывать новые позиции или только закрываем уже существующие

    main(demo, open_new_orders, max_position, min_order, max_pairs, leverage, fee_rate, td)
