from jaref_bot.analysis.backtest.pair_trading import make_df_from_orderbooks, backtest
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import polars as pl
from tqdm import tqdm
import heapq
from random import choice

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name
db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI

def find_best_params(df, token_1, token_2, dp_1, dp_2, ps_1, ps_2,
                     low_in_params, high_in_params, low_out_params,
                     high_out_params, stop_loss_std=5.0, sl_method=None,
                     method_in='direct', method_out='direct', min_trades=10,
                     leverage=1,
                     n_best_params=3,
                     verbose=0):
    heap = []

    for thresh_low_in in low_in_params:
        for thresh_high_in in high_in_params:
            for thresh_low_out in low_out_params:
                for thresh_high_out in high_out_params:
                    if abs(thresh_high_out) > abs(thresh_low_in):
                        continue
                    if abs(thresh_low_out) > abs(thresh_high_in):
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

                    # print(tr)
                    if tr.height >= min_trades:
                        profit = tr['total_profit'].sum()
                        pars = (thresh_low_in,
                                thresh_high_in,
                                thresh_low_out,
                                thresh_high_out)
                        if len(heap) < n_best_params:
                            heapq.heappush(heap, (profit, tr.height, pars))
                        else:
                            if profit > heap[0][0]:
                                heapq.heapreplace(heap, (profit, tr.height, pars))
                    else:
                        profit = 0
    return heap

def random_search(token_1, token_2, start_time, end_time, min_trades, n_top_params,
        search_space, low_in_params, high_in_params, low_out_params, high_out_params,
        leverage, n_iters=100,
        verbose=0):
    method = 'dist'
    top_params = []

    # Загружаем датафрейм с рассчитанным спредом и z_score
    spread_df = pl.read_parquet(f'./data/{token_1}_{token_2}_{method}.parquet')

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
        thresh_low_in = choice(low_in_params)
        thresh_low_out = choice(low_out_params)
        thresh_high_in = choice(high_in_params)
        thresh_high_out = choice(high_out_params)
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

#         print(f'Profit: {profit:.2f}; n_trades: {tr.height}; tf: {tf}; wind: {wind}; \
# method_in: {method_in}; method_out: {method_out}; params: {pars}')

    print(f'===== Top {n_top_params} params =====')
    for p in top_params:
        print(f'Profit: {p[0]:.2f}; n_tr: {p[1]}; {p[2]}; {p[3]}; \
in: {p[4]}; out: {p[5]}; {p[6]}')

    db_manager.close()



def main(token_1, token_2, start_time, end_time, min_trades, n_top_params,
        search_space, low_in_params, high_in_params, low_out_params, high_out_params,
        leverage, verbose=0):
    method = 'dist'
    top_params = []

    # Загружаем датафрейм с рассчитанным спредом и z_score
    spread_df = pl.read_parquet(f'./data/{token_1}_{token_2}_{method}.parquet')

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

    for tf, wind in search_space:
        print(f'Параметры модели. tf: {tf}, wind: {wind}')

        try:
            df = spread_df.select('time', 'ts', 'spread', f'z_score_{wind}_{tf}')
            df = df.rename({f'z_score_{wind}_{tf}': 'z_score'})
            df = df.join(bid_ask_df, on='ts')
        except pl.exceptions.ColumnNotFoundError:
            print('Нет такого временного окна в датафрейме\n')
            continue

        best_params = find_best_params(df, token_1, token_2,
                dp_1, dp_2, ps_1, ps_2, n_best_params=3,
                low_in_params=low_in_params, high_in_params=high_in_params,
                low_out_params=low_out_params, high_out_params=high_out_params,
                stop_loss_std=5.0, sl_method='leave', leverage=leverage,
                method_in='direct', method_out='direct', min_trades=min_trades,
                verbose=verbose)
        for profit, n_trades, params in best_params:
            print(f'profit: {profit:.2f}. {n_trades=}; params: {params}')

            if len(top_params) < n_top_params:
                heapq.heappush(top_params, (profit, n_trades, tf, wind, params))
            else:
                if profit > top_params[0][0]:
                    heapq.heapreplace(top_params, (profit, n_trades, tf, wind, params))

        print()
    print(f'===== Top {n_top_params} params =====')
    for p in top_params:
        print(f'Profit: {p[0]:.2f}; n_trades: {p[1]}; tf: {p[2]}; wind: {p[3]}; params: {p[4]}')

    db_manager.close()


if __name__ == '__main__':
    token_1 = 'CELO'
    token_2 = 'GRT'
    start_time = datetime(2025, 8, 26, 12, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    end_time = datetime(2025, 9, 1, 22, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    min_trades = 6
    n_top_params = 10
    leverage = 1

    # Зададим пространство поиска наилучших параметров входа
    search_space = (
            ('4h', 8), ('4h', 10), ('4h', 12), ('4h', 16), ('4h', 20),
            ('1h', 12), ('1h', 18), ('1h', 24), ('1h', 36), ('1h', 48), ('1h', 60),
            ('15m', 15), ('15m', 30), ('15m', 45),
            ('15m', 60), ('15m', 80), ('15m', 100), ('15m', 120), ('15m', 160),
            ('5m', 60), ('5m', 90), ('5m', 120), ('5m', 240), ('5m', 360), ('5m', 480)
        )

    low_in_params = (-0.8, -1.0, -1.2, -1.4, -1.6, -1.8, -2.0, -2.2, -2.4, -2.6, -2.8, -3.0)
    high_in_params = (0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0)
    low_out_params = (-0.25, -0.5, -0.8, -1.0, -1.2, -1.4, -1.6, -1.8, -2.0, -2.2, -2.4, -2.6, -2.8, -3.0)
    high_out_params = (0.25, 0.5, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0)

    main(token_1, token_2, start_time, end_time, min_trades, n_top_params,
        search_space, low_in_params, high_in_params, low_out_params, high_out_params,
        leverage, verbose=0)

    # random_search(token_1, token_2, start_time, end_time, min_trades, n_top_params,
    #     search_space, low_in_params, high_in_params, low_out_params, high_out_params,
    #     leverage, n_iters=100_000, verbose=0)
