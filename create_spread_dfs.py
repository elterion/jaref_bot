from jaref_bot.utils.pair_trading import make_df_from_orderbooks, make_trunc_df, create_zscore_df
from datetime import datetime
from zoneinfo import ZoneInfo
import polars as pl
import numpy as np
from tqdm import tqdm

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name
db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI

def main(spread_method, tf, winds, start_time, valid_time, end_time, min_order):
    cointegrated_tokens = []
    with open('./jaref_bot/config/cointegrated_tokens.txt', 'r') as file:
        for line in file:
            a, b = line.strip().split()
            cointegrated_tokens.append((a, b))

    for token_1, token_2 in tqdm(cointegrated_tokens):
        if spread_method == 'lr':
            return_spread = False
            log_spread = False
        else:
            return_spread = True
            log_spread = True

        t1_name = token_1 + '_USDT'
        t2_name = token_2 + '_USDT'

        token_1_first_date = db_manager.get_oldest_date_in_orderbook(t1_name)
        token_2_first_date = db_manager.get_oldest_date_in_orderbook(t2_name)

        if token_1_first_date > start_time or token_2_first_date > start_time:
            tqdm.write(f'Для пары {token_1} - {token_2} не хватает тренировочной выборки.')
            continue

        df_1 = db_manager.get_tick_ob(token=t1_name,
                                        start_time=start_time,
                                        end_time=end_time)
        df_2 = db_manager.get_tick_ob(token=t2_name,
                                        start_time=start_time,
                                        end_time=end_time)

        df = make_df_from_orderbooks(df_1, df_2, token_1, token_2, start_time=start_time,
                                return_spread=return_spread, log_spread=log_spread)

        agg_df = make_trunc_df(df, timeframe=tf, token_1=token_1, token_2=token_2, method='triple')
        tick_df = make_df_from_orderbooks(df_1, df_2, token_1, token_2, start_time=start_time,
                                return_spread=return_spread, log_spread=log_spread)

        start_ts = int(datetime.timestamp(valid_time))
        spread_df = create_zscore_df(token_1, token_2, tick_df, agg_df, tf, winds,
                                     min_order, start_ts, median_length=6)

        spread_df.write_parquet(f'./data/pair_backtest/{token_1}_{token_2}_{tf}_{spread_method}.parquet')

if __name__ == '__main__':
    spread_method = 'lr'
    tf = '4h'
    # short_tf = '5m'
    min_order = 40
    winds = np.array([8, 10, 12, 14, 16, 18, 24])
    # winds = np.array([30, 45, 60, 90, 120, 180, 240, 300])

    # end_time = datetime.now(ZoneInfo("Europe/Moscow"))
    end_time = datetime(2025, 9, 26, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    valid_time = datetime(2025, 9, 16, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))
    start_time = datetime(2025, 9, 6, 0, 0, tzinfo=ZoneInfo("Europe/Moscow"))

    # main(spread_method, tf, winds, start_time, valid_time, end_time, min_order)

    for tf, winds in (('4h', np.array([8, 10, 12, 14, 16, 18, 24, 30])),
                      ('1h', np.array([12, 18, 24, 36, 48, 64, 72, 96, 120, 240])),
                      ('5m', np.array([30, 45, 60, 90, 120, 180, 240, 300]))):
        main(spread_method, tf, winds, start_time, valid_time, end_time, min_order)

    db_manager.close()
