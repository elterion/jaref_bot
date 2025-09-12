from jaref_bot.analysis.backtest.pair_trading import backtest
from jaref_bot.utils.pair_trading import make_df_from_orderbooks, make_trunc_df, get_zscore
from jaref_bot.utils.pair_trading import create_zscore_df
from jaref_bot.analysis.strategy_analysis import analyze_strategy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import polars as pl
import numpy as np
from tqdm import tqdm
import heapq
from random import choice
import pickle

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name
db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI

def base(token: str) -> str:
    return token.split('_')[0] if '_' in token else token

def find_best_params(df, token_1, token_2, dp_1, dp_2, ps_1, ps_2,
                     in_params, out_params,
                     stop_loss_std=5.0, sl_method=None,
                     method_in='direct', method_out='direct', min_trades=10,
                     leverage=1, n_best_params=2, verbose=0):
    """
    Параметры in_params и out_params передаются в виде положительных чисел.

    """

    heap = []
    end_date = df['time'][-1]
    start_date = df['time'][0]

    order_size = 100
    balance = order_size * 2
    # max_dist = 0.3 # Разброс параметров входа/выхода.
    # Вводится, чтобы не добавлять параметры в духе (-2.8, 0.6, 1.6, -1.8)

    for thresh_in in in_params:
        for thresh_out in out_params:
            thresh_low_in = -thresh_in
            thresh_high_in = thresh_in
            thresh_low_out = -thresh_out
            thresh_high_out = thresh_out

            if thresh_low_out < thresh_low_in:
                continue
            if thresh_high_out > thresh_high_in:
                continue


            tr = backtest(df, token_1, token_2, dp_1, dp_2, ps_1, ps_2,
                thresh_low_in=thresh_low_in, thresh_high_in=thresh_high_in,
                thresh_low_out=thresh_low_out, thresh_high_out=thresh_high_out,
                long_possible=True, short_possible=True,
                balance=balance, order_size=order_size, fee_rate=0.00055,
                method_in=method_in, method_out=method_out,
                stop_loss_std=stop_loss_std, sl_method=sl_method,
                leverage=leverage
                )

            if tr.height >= min_trades:
                # profit = tr['total_profit'].sum()
                metrics = analyze_strategy(tr, start_date=start_date,
                                            end_date=end_date,
                                            initial_balance=balance)
                profit_ratio = metrics['profit_ratio']

                # dist = max(map(abs, pars)) - min(map(abs, pars))
                if len(heap) < n_best_params:
                    heapq.heappush(heap, (profit_ratio, tr.height, thresh_in, thresh_out))
                else:
                    if profit_ratio > heap[0][0]:
                        heapq.heapreplace(heap, (profit_ratio, tr.height, thresh_in, thresh_out))
    return heap

def random_search(token_1, token_2, method, start_time, end_time, min_trades, n_top_params,
        search_space, in_params, out_params,
        leverage, n_iters=100,
        verbose=0):
    top_params = []

    # Загружаем датафрейм с рассчитанным спредом и z_score
    spread_df = pl.read_parquet(f'./data/pair_backtest/{token_1}_{token_2}_{method}.parquet')

    # Загружаем датафреймы с ценами
    df_1 = db_manager.get_raw_orderbooks(exchange='bybit',
                                     market_type='linear',
                                     token=token_1 + '_USDT',
                                     start_time=start_time,
                                     end_time=end_time)
    df_1 = df_1.with_columns(pl.col('time').dt.epoch('s').alias('ts'))
    df_2 = db_manager.get_raw_orderbooks(exchange='bybit',
                                        market_type='linear',
                                        token=token_2 + '_USDT',
                                        start_time=start_time,
                                        end_time=end_time)
    df_2 = df_2.with_columns(pl.col('time').dt.epoch('s').alias('ts'))

    bid_ask_df = make_df_from_orderbooks(df_1, df_2, token_1, token_2, start_time, end_time)
    bid_ask_df = bid_ask_df.select('ts', f'{token_1}_bid_price',
                                f'{token_1}_ask_price',
                                f'{token_2}_bid_price',
                                f'{token_2}_ask_price'
                                )

    # Загружаем с биржи ByBit техническую информацию по монетам (шаг цены, округление цены в usdt etc.)
    exc_manager = ExchangeManager()
    exc_manager.add_market("bybit_linear", BybitRestAPI('linear'))
    coin_information = exc_manager.get_instrument_data()

    # Сохраним информацию о шаге цены монет в переменных
    dp_1 = float(coin_information['bybit_linear'][token_1 + '_USDT']['qty_step'])
    ps_1 = int(coin_information['bybit_linear'][token_1 + '_USDT']['price_scale'])
    dp_2 = float(coin_information['bybit_linear'][token_2 + '_USDT']['qty_step'])
    ps_2 = int(coin_information['bybit_linear'][token_2 + '_USDT']['price_scale'])

    for _ in tqdm(range(n_iters)):
        tf, wind = choice(search_space)
        thresh_low_in = choice([-x for x in in_params])
        thresh_low_out = choice([-x for x in out_params])
        thresh_high_in = choice(in_params)
        thresh_high_out = choice(out_params)
        method_in = choice(['direct', 'reverse'])
        method_out = choice(['direct', 'reverse'])
        sl_method = 'leave'
        stop_loss_std = 5.0

        if abs(thresh_high_out) > abs(thresh_low_in):
            continue
        if abs(thresh_low_out) > abs(thresh_high_in):
            continue

        try:
            df = spread_df.select('time', 'ts', 'spread', f'z_score_{wind}_{tf}')
            df = df.rename({f'z_score_{wind}_{tf}': 'z_score'})
            df = df.join(bid_ask_df, on='ts')
        except pl.exceptions.ColumnNotFoundError:
            continue

        tr = backtest(df, token_1, token_2, dp_1, dp_2, ps_1, ps_2,
            thresh_low_in=thresh_low_in, thresh_high_in=thresh_high_in,
            thresh_low_out=thresh_low_out, thresh_high_out=thresh_high_out,
            long_possible=True, short_possible=True,
            balance=1000, order_size=100, fee_rate=0.00055,
            method_in=method_in, method_out=method_out,
            stop_loss_std=stop_loss_std, sl_method=sl_method,
            leverage=leverage
            )

        pars = ()
        if tr.height >= min_trades:
            profit = tr['total_profit'].sum()
            pars = (thresh_low_in, thresh_high_in, thresh_low_out, thresh_high_out)

            if len(top_params) < n_top_params:
                heapq.heappush(top_params, (profit, tr.height, tf, wind,
                                            method_in, method_out, pars))
            else:
                if profit > top_params[0][0]:
                    heapq.heapreplace(top_params, (profit, tr.height, tf, wind,
                                                   method_in, method_out, pars))
        else:
            profit = 0

    print(f'===== Top {n_top_params} params =====')
    for p in top_params:
        print(f'Profit: {p[0]:.2f}; n_tr: {p[1]}; {p[2]}; {p[3]}; \
in: {p[4]}; out: {p[5]}; {p[6]}')

def grid_search(token_1, token_2, method, start_time, end_time, min_trades, n_top_params,
        search_space, method_in, in_params, out_params,
        leverage, verbose=0):

    top_params = []

    # Загружаем датафрейм с рассчитанным спредом и z_score
    spread_df = pl.read_parquet(f'./data/pair_backtest/{token_1}_{token_2}_{method}.parquet').filter(
        (pl.col('time') >= start_time) & (pl.col('time') < end_time)
    )

    # Загружаем техническую информацию по монетам (шаг цены, округление цены в usdt etc.)
    with open("./data/coin_information.pkl", "rb") as f:
        coin_information = pickle.load(f)

    # Сохраним информацию о шаге цены монет в переменных
    dp_1 = float(coin_information['bybit_linear'][token_1 + '_USDT']['qty_step'])
    ps_1 = int(coin_information['bybit_linear'][token_1 + '_USDT']['price_scale'])
    dp_2 = float(coin_information['bybit_linear'][token_2 + '_USDT']['qty_step'])
    ps_2 = int(coin_information['bybit_linear'][token_2 + '_USDT']['price_scale'])

    for tf, wind in search_space:
        if verbose > 0:
            print(f'Параметры модели. tf: {tf}, wind: {wind}')

        try:
            df = spread_df.select('time', 'ts', token_1, token_2, f'{token_1}_size', f'{token_2}_size',
                 f'{token_1}_bid_price', f'{token_1}_ask_price', f'{token_1}_bid_size', f'{token_1}_ask_size',
                 f'{token_2}_bid_price', f'{token_2}_ask_price', f'{token_2}_bid_size', f'{token_2}_ask_size',
                 f'z_score_{wind}_{tf}')
            df = df.rename({f'z_score_{wind}_{tf}': 'z_score'})
        except pl.exceptions.ColumnNotFoundError:
            print('Нет такого временного окна в датафрейме\n')
            continue

        best_params = find_best_params(df, token_1, token_2,
                dp_1, dp_2, ps_1, ps_2, n_best_params=3,
                in_params=in_params, out_params=out_params,
                stop_loss_std=5.0, sl_method='leave', leverage=leverage,
                method_in=method_in, method_out='direct', min_trades=min_trades,
                verbose=verbose)
        for profit, n_trades, thresh_in, thresh_out in best_params:
            if verbose > 0:
                print(f'profit: {profit:.2f}. {n_trades=}; params: {thresh_in, thresh_out}')

            if len(top_params) < n_top_params:
                heapq.heappush(top_params, (profit, n_trades, tf, wind, thresh_in, thresh_out))
            else:
                if profit > top_params[0][0]:
                    heapq.heapreplace(top_params, (profit, n_trades, tf, wind, thresh_in, thresh_out))

        if verbose > 0:
            print()
    # print(f'===== Top {n_top_params} params =====')
    for p in top_params:
        tqdm.write(f'({p[0]:.2f}, "{token_1}", "{token_2}", "{p[2]}", {p[3]}, {p[4]}, {p[5]})')

    return top_params

def create_data(token_1, token_2, method, start_time, valid_time,
              hour4_winds, hour1_winds, min_order, write_to_file=True):
    if method == 'lr':
        return_spread = False
        log_spread = False
    else:
        return_spread = True
        log_spread = True

    df_1 = db_manager.get_raw_orderbooks(exchange='bybit',
                                     market_type='linear',
                                     token=token_1 + '_USDT')
    df_2 = db_manager.get_raw_orderbooks(exchange='bybit',
                                        market_type='linear',
                                        token=token_2 + '_USDT')
    df = make_df_from_orderbooks(df_1, df_2, token_1, token_2, start_time=start_time,
                             return_spread=return_spread, log_spread=log_spread)

    df_hour = make_trunc_df(df, timeframe='1h', token_1=token_1, token_2=token_2, method='triple')
    df_4hour = make_trunc_df(df, timeframe='4h', token_1=token_1, token_2=token_2, method='triple')
    df_sec = make_trunc_df(df, timeframe='1s', token_1=token_1, token_2=token_2,
                           start_date=valid_time, method='last', return_bid_ask=True)

    res_df = create_zscore_df(token_1, token_2, df_sec, df_4hour, df_hour, hour4_winds, hour1_winds, method, min_order)
    if write_to_file:
        res_df.write_parquet(f'./data/pair_backtest/{token_1}_{token_2}_{method}.parquet')

    return res_df

def main(token_1, token_2, method, create_df_flag, method_in,
         start_time, valid_time, end_time, min_trades, n_top_params, leverage,
         search_type='grid', min_order=50, n_iters=100_000, verbose=0, save_to_file=False):

    hour4_winds = np.array([8, 10, 12, 14, 16, 18])
    hour1_winds = np.array([12, 18, 24, 32, 40, 48])

    # Создадим датафрейм с z_score
    if create_df_flag:
        create_data(token_1, token_2, method, start_time, valid_time,
                hour4_winds, hour1_winds, min_order)

    # Зададим пространство поиска наилучших параметров входа
    search_space = [('4h', w) for w in hour4_winds] + [('1h', w) for w in hour1_winds]

    in_params = (1.8, 2.0)
    out_params = (0.25, )

    if search_type == 'grid':
        top_params = grid_search(token_1, token_2, method, valid_time, end_time, min_trades, n_top_params,
            search_space, method_in, in_params, out_params,
            leverage, verbose=verbose)
    elif search_type == 'random':
        random_search(token_1, token_2, method, valid_time, end_time, min_trades, n_top_params,
            search_space, in_params, out_params,
            leverage, n_iters=n_iters, verbose=verbose)

    if save_to_file:
        with open('./jaref_bot/config/thresholds.txt', 'w') as file:
            for p in top_params:
                file.write(f'({p[0]:.2f}, "{p[1]}", "{p[2]}", "{p[3]}", {p[4]}, {p[5]}, {p[6]})\n')

if __name__ == '__main__':
    create_df_flag = True
    method = 'lr'
    valid_length = 3
    train_length = 6

    # end_time = datetime.now(ZoneInfo("Europe/Moscow"))
    end_time = datetime(2025, 9, 12, 23, 30, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    valid_time = (end_time - timedelta(days=valid_length)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    start_time = valid_time - timedelta(days=train_length)

    min_trades = 2
    n_top_params = 1 # Сколько лучших параметров печатать на экране
    leverage = 2
    min_order = 50

    method_in = 'direct'

    cointegrated_tokens = []
    with open('./jaref_bot/config/cointegrated_tokens.txt', 'r') as file:
        for line in file:
            a, b = line.strip().split()
            cointegrated_tokens.append((a, b))

    # --- Проверка того, все ли открытые позиции есть в cointegrated_tokens ---
    current_pairs = db_manager.get_table('pairs', df_type='polars')
    cointegrated_set_base = set((a, b) for a, b in cointegrated_tokens)

    missing_pairs = [
        (base(r['token_1']), base(r['token_2']))
        for r in current_pairs.to_dicts()
        if (base(r['token_1']), base(r['token_2'])) not in cointegrated_set_base
    ]

    for pair in missing_pairs:
        cointegrated_tokens.append(pair)


    # --- Бектест по всем коинтегрированным парам токенов ---
    for token_1, token_2 in tqdm(cointegrated_tokens):
        # tqdm.write(f'\n===== {token_1} - {token_2} =====')
        main(token_1, token_2, method, create_df_flag, method_in,
            start_time, valid_time, end_time, min_trades, n_top_params, leverage,
            search_type='grid', min_order=min_order, verbose=0, save_to_file=True)

    db_manager.close()
