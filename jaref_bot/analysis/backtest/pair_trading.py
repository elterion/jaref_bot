import polars as pl
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from sklearn.preprocessing import StandardScaler
import numpy as np
from numba import njit
from numba.typed import List as NumbaList

from jaref_bot.data.http_api import ExchangeManager, BybitRestAPI

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name
db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

SIG_NONE, SIG_LONG_OPEN, SIG_SHORT_OPEN, SIG_LONG_CLOSE, SIG_SHORT_CLOSE = 0, 1, 2, 3, 4
POS_NONE, POS_LONG, POS_SHORT = 0, 1, 2
REASON_NONE, REASON_THRESHOLD, REASON_STOPLOSS, REASON_LIQ = 0, 1, 2, 3
LIQ_NONE, LIQ_LONG, LIQ_SHORT = 0, 1, 2  # для внутренней логики
EV_TYPE_OPEN, EV_TYPE_CLOSE, EV_TYPE_SL, EV_TYPE_LIQ = 1, 2, 3, 4

try:
    # Проверяем, есть ли IPython и работаем ли мы в ноутбуке
    from IPython import get_ipython
    shell = get_ipython().__class__.__name__
    if shell == "ZMQInteractiveShell":  # Jupyter Notebook или JupyterLab
        from tqdm.notebook import tqdm
    else:  # IPython в терминале или обычный Python
        from tqdm import tqdm
except Exception:
    # Если IPython не установлен — значит точно CLI
    from tqdm import tqdm

def make_trunc_df(df, timeframe, token_1, token_2, start_date=None, end_date=None, method="last"):
    df = df.with_columns(
            ((pl.col(f'{token_1}_bid_price') + pl.col(f'{token_1}_ask_price')) / 2).alias(token_1),
            ((pl.col(f'{token_2}_bid_price') + pl.col(f'{token_2}_ask_price')) / 2).alias(token_2),
        ).select('time', 'ts', token_1, token_2, 'spread')

    # условия агрегации
    if method == "last":
        agg_exprs = [
            pl.col("ts").last(),
            pl.col(token_1).last().alias(token_1),
            pl.col(token_2).last().alias(token_2),
            pl.col("spread").last().alias("spread"),
        ]
    elif method == "triple":
        agg_exprs = [
            pl.col("ts").last(),
            ((pl.col(token_1).last() + pl.col(token_1).max() + pl.col(token_1).min()) / 3).alias(token_1),
            ((pl.col(token_2).last() + pl.col(token_2).max() + pl.col(token_2).min()) / 3).alias(token_2),
            ((pl.col("spread").last() + pl.col("spread").max() + pl.col("spread").min()) / 3).alias("spread"),
        ]
    else:
        raise ValueError(f"Unknown method: {method}")

    df = df.group_by_dynamic(
                index_column="time",
                every=timeframe,
                label='right'
            ).agg(agg_exprs)
    if start_date:
        df = df.filter(pl.col('time') > start_date)
    if end_date:
        df = df.filter(pl.col('time') < end_date)

    return df

def make_df_from_orderbooks(df_1, df_2, token_1, token_2, start_time=None, end_time=None, log_=True):
    """
    Функция на вход принимает 2 датафрейма с ордербуками, усредняет цены покупки/продажи
    и возвращает новый датафрейм с рассчитанным спредом.

    """

    date_col = 'bucket' if 'bucket' in df_1.columns else 'time'

    if start_time is None:
        start_time = df_1[date_col].min()
    if end_time is None:
        end_time = df_1[date_col].max()

    df = df_1.drop('exchange', 'market_type', 'token'
        ).rename(
            {'bid_price': f'{token_1}_bid_price', 'bid_size': f'{token_1}_bid_size',
             'ask_price': f'{token_1}_ask_price', 'ask_size': f'{token_1}_ask_size'}
        ).join(df_2.drop('exchange', 'market_type', 'token'),
            on=date_col, how='full', coalesce=True, suffix='_r'
        ).sort(by=date_col
        ).rename(
            {'bid_price': f'{token_2}_bid_price', 'bid_size': f'{token_2}_bid_size',
             'ask_price': f'{token_2}_ask_price', 'ask_size': f'{token_2}_ask_size'}
        ).fill_null(strategy='forward'
        ).filter(
            (pl.col(date_col) > start_time) & (pl.col(date_col) < end_time)
        ).with_columns(
            pl.col(date_col).dt.epoch('s').alias('ts'),
            ((pl.col(f'{token_1}_bid_price') + pl.col(f'{token_1}_ask_price')) / 2).alias(token_1),
            ((pl.col(f'{token_2}_bid_price') + pl.col(f'{token_2}_ask_price')) / 2).alias(token_2),
        )

    if log_:
        df = df.with_columns(
            (pl.col(token_1).log() - pl.col(token_2).log()).alias('spread')
        )
    else:
        df = df.with_columns(
            (pl.col(token_1) - pl.col(token_2)).alias('spread')
        )

    return df

def make_spread_df(df, token_1, token_2, wind):
    return df.lazy().with_columns(
                pl.col('spread').rolling_mean(wind).alias(f'mean'),
                pl.col('spread').rolling_std(wind, ddof=1).alias(f'std')
            ).with_columns(
                ((pl.col('spread') - pl.col('mean')) / pl.col('std')).alias(f'z_score')
            ).collect()

def make_spread_df_bulk(df, token_1, token_2, winds):
    exprs = []

    for wind in winds:
        mean_col = pl.col('spread').rolling_mean(wind).alias(f'mean_{wind}')
        std_col = pl.col('spread').rolling_std(wind).alias(f'std_{wind}')
        z_score_col = ((pl.col('spread') - mean_col) / std_col).alias(f'z_score_{wind}')

        exprs.extend([mean_col, std_col, z_score_col])

    return df.lazy().with_columns(exprs).collect()

@njit("float64(float64, float64)", fastmath=True, cache=True)
def _round(value, dp):
    return np.floor(value / dp) * dp

@njit(fastmath=True, cache=True)
def backtest_fast(time_arr, z_score, bid_1, ask_1, bid_2, ask_2,
                  dp_1, dp_2, ps_1, ps_2, thresh_low_in, thresh_low_out,
             thresh_high_in, thresh_high_out, long_possible, short_possible,
             reverse_in, reverse_out,
             balance, order_size, fee_rate,  stop_loss_std, sl_method,
             sl_seconds=0,
             leverage=1):

    n = z_score.shape[0]
    total_balance = balance

    signal = SIG_NONE # Текущее состояние ордера
    reason = REASON_NONE # Причина закрытия сделки
    pos_side = POS_NONE

    open_time = 0
    open_price_1 = 0.0
    open_price_2 = 0.0
    qty_1 = 0.0
    qty_2 = 0.0

    liq_status = LIQ_NONE
    long_flag_in = 0
    short_flag_in = 0
    long_flag_out = 0
    short_flag_out = 0
    sl_counter = 0
    sl_block_long = 0
    sl_block_short = 0

    out = NumbaList()
    events = NumbaList()

    for i in range(n):
        z = z_score[i]
        if total_balance < 0:
            return out, events

        # --- Проверка ликвидации позиции ---
        if pos_side != POS_NONE:
            if pos_side == POS_LONG:
                long_liq_price = open_price_1 * (1 - 1/leverage)
                short_liq_price = open_price_2 * (1 + 1/leverage)
                long_price = bid_1[i]    # цена закрытия long — бид по token_1
                short_price = ask_2[i]   # цена закрытия short — аск по token_2
            elif pos_side == POS_SHORT:
                long_liq_price = open_price_2 * (1 - 1/leverage)
                short_liq_price = open_price_1 * (1 + 1/leverage)
                long_price = bid_2[i]    # цена закрытия long — бид по token_2
                short_price = ask_1[i]   # цена закрытия short — аск по token_1

            if short_price > short_liq_price:
                liq_status = LIQ_SHORT
            elif long_price < long_liq_price:
                liq_status = LIQ_LONG

            if liq_status != LIQ_NONE:
                # Рассчитываем цены закрытия по стандартной логике стороны:
                if pos_side == POS_LONG:
                    price_1 = bid_1[i]
                    price_2 = ask_2[i]
                else:  # POS_SHORT
                    price_1 = ask_1[i]
                    price_2 = bid_2[i]

                usdt_open_1 = qty_1 * open_price_1
                usdt_open_2 = qty_2 * open_price_2
                open_fee_1 = usdt_open_1 * fee_rate
                open_fee_2 = usdt_open_2 * fee_rate

                usdt_close_1 = qty_1 * price_1
                usdt_close_2 = qty_2 * price_2
                close_fee_1 = usdt_close_1 * fee_rate
                close_fee_2 = usdt_close_2 * fee_rate

                if pos_side == POS_LONG:
                    profit_1 = usdt_close_1 - usdt_open_1 - open_fee_1 - close_fee_1
                    profit_2 = usdt_open_2 - usdt_close_2 - open_fee_2 - close_fee_2
                else:
                    profit_1 = usdt_open_1 - usdt_close_1 - open_fee_1 - close_fee_1
                    profit_2 = usdt_close_2 - usdt_open_2 - open_fee_2 - close_fee_2

                if liq_status == LIQ_LONG:
                    profit_1 = -usdt_open_1  # потеря всей long-ноги
                    close_fee_1 = 0.0
                elif liq_status == LIQ_SHORT:
                    profit_2 = -usdt_open_2  # потеря всей short-ноги
                    close_fee_2 = 0.0

                fees = open_fee_1 + open_fee_2 + close_fee_1 + close_fee_2
                total_profit = profit_1 + profit_2
                total_balance += total_profit

                reason = REASON_LIQ

                events.append((EV_TYPE_LIQ, open_time, time_arr[i], qty_1, qty_2,
                           open_price_1, price_1, open_price_2, price_2, pos_side,
                           fees, profit_1, profit_2, total_profit, reason))

                out.append((open_time, time_arr[i], qty_1, qty_2,
                          open_price_1, price_1, open_price_2, price_2,
                          pos_side, fees, profit_1, profit_2, total_profit,
                          reason))

                # Сброс состояния
                signal = SIG_NONE
                reason = REASON_NONE
                pos_side = POS_NONE
                open_time = 0
                open_price_1 = 0.0
                open_price_2 = 0.0
                qty_1 = 0.0
                qty_2 = 0.0
                liq_status = LIQ_NONE

        # --- Открываем ордер, если есть команда на открытие ---
        if signal == SIG_LONG_OPEN:
            open_time = time_arr[i]
            open_price_1 = ask_1[i]
            open_price_2 = bid_2[i]
            qty_1 = _round(leverage * order_size / (1.0 + 2.0 * fee_rate) / open_price_1, dp_1)
            qty_2  = _round(leverage * order_size / (1.0 + 2.0 * fee_rate) / open_price_2, dp_2)
            pos_side = POS_LONG
            signal = SIG_NONE

            events.append((EV_TYPE_OPEN, open_time, 0, qty_1, qty_2,
                           open_price_1, 0.0, open_price_2, 0.0, pos_side,
                           0.0, 0.0, 0.0, 0.0, 0))

        elif signal == SIG_SHORT_OPEN:
            open_time = time_arr[i]
            open_price_1 = bid_1[i]
            open_price_2 = ask_2[i]
            qty_1 = _round(leverage * order_size / (1.0 + 2.0 * fee_rate) / open_price_1, dp_1)
            qty_2 = _round(leverage * order_size / (1.0 + 2.0 * fee_rate) / open_price_2, dp_2)
            pos_side = POS_SHORT
            signal = SIG_NONE

            events.append((EV_TYPE_OPEN, open_time, 0, qty_1, qty_2,
                           open_price_1, 0.0, open_price_2, 0.0, pos_side,
                           0.0, 0.0, 0.0, 0.0, 0))

        # --- Закрываем ордер, если есть команда на закрытие ---
        elif signal == SIG_LONG_CLOSE or signal == SIG_SHORT_CLOSE:
            if signal == SIG_LONG_CLOSE:
                price_1 = bid_1[i]
                price_2 = ask_2[i]
            else:  # SIG_SHORT_CLOSE
                price_1 = ask_1[i]
                price_2 = bid_2[i]

            usdt_open_1 = qty_1 * open_price_1
            usdt_open_2 = qty_2 * open_price_2
            open_fee_1 = usdt_open_1 * fee_rate
            open_fee_2 = usdt_open_2 * fee_rate

            usdt_close_1 = qty_1 * price_1
            usdt_close_2 = qty_2 * price_2
            close_fee_1 = usdt_close_1 * fee_rate
            close_fee_2 = usdt_close_2 * fee_rate

            fees = open_fee_1 + open_fee_2 + close_fee_1 + close_fee_2

            if signal == SIG_LONG_CLOSE:
                profit_1 = usdt_close_1 - usdt_open_1 - open_fee_1 - close_fee_1
                profit_2 = usdt_open_2 - usdt_close_2 - open_fee_2 - close_fee_2
            else:
                profit_1 = usdt_open_1 - usdt_close_1 - open_fee_1 - close_fee_1
                profit_2 = usdt_close_2 - usdt_open_2 - open_fee_2 - close_fee_2

            total_profit = profit_1 + profit_2
            total_balance += total_profit

            reas = EV_TYPE_CLOSE if reason == REASON_THRESHOLD else EV_TYPE_SL

            events.append((reas, open_time, time_arr[i], qty_1, qty_2,
                           open_price_1, price_1, open_price_2, price_2, pos_side,
                           fees, profit_1, profit_2, total_profit, reason))
            out.append((
                open_time, time_arr[i],
                qty_1, qty_2,
                open_price_1, price_1,
                open_price_2, price_2,
                pos_side,
                fees,
                profit_1, profit_2, total_profit,
                reason
            ))


            # Сброс
            signal = SIG_NONE
            reason = REASON_NONE
            pos_side = POS_NONE
            open_time = 0
            open_price_1 = 0.0
            open_price_2 = 0.0
            qty_1 = 0.0
            qty_2 = 0.0

        # --- Проверяем действующий стоп-лосс счётчик ---
        if sl_counter > 0:
            sl_counter -= 1
            continue


        # --- Проверяем условие входа в сделку ---
        if pos_side == POS_NONE:
            # Проверяем блокировку после стоп-лосса
            if sl_method == 2:
                # Разблокируем стоп-лосс при выходе из зоны лонг-ставки
                if sl_block_long:
                    if z > thresh_low_in:
                        sl_block_long = 0
                # Разблокируем стоп-лосс при выходе из зоны шорт-ставки
                if sl_block_short:
                    if z < thresh_high_in:
                        sl_block_short = 0

            # Прямой способ входа (когда z_score входит в диапазон входа)
            if reverse_in == 0:
                if z < thresh_low_in and long_possible and not sl_block_long:
                    signal = SIG_LONG_OPEN
                elif z > thresh_high_in and short_possible and not sl_block_short:
                    signal = SIG_SHORT_OPEN
            # Обратный способ входа (когда z_score выходит из диапазона входа)
            else:
                # Long
                if not long_flag_in and z < thresh_low_in and long_possible and not sl_block_long:
                    long_flag_in = 1
                    # print('Инициируем флаг входа в лонг', time_arr[i], z)
                elif long_flag_in and z > thresh_low_in and long_possible and not sl_block_long:
                    signal = SIG_LONG_OPEN
                    long_flag_in = 0
                    # print('Входим в лонг', time_arr[i], z)
                # Short
                if not short_flag_in and z > thresh_high_in and short_possible and not sl_block_short:
                    short_flag_in = 1
                    # print('Инициируем флаг входа в шорт', time_arr[i], z)
                elif short_flag_in and z < thresh_high_in and short_possible and not sl_block_short:
                    short_flag_in = 0
                    signal = SIG_SHORT_OPEN
                    # print('Входим в шорт', time_arr[i], z)

        # --- Проверяем условие выхода из сделки ---
        if pos_side == POS_SHORT:
            # Прямой способ (когда z_score входит в диапазон выхода)
            if not reverse_out:
                if z < thresh_low_out:
                    signal = SIG_SHORT_CLOSE
                    reason = REASON_THRESHOLD
            # Выходим из сделки, когда z_score покидает диапазон выхода
            else:
                if not short_flag_out and z < thresh_low_out:
                    short_flag_out = 1
                elif short_flag_out and z > thresh_low_out:
                    signal = SIG_SHORT_CLOSE
                    reason = REASON_THRESHOLD
                    short_flag_out = 0

        elif pos_side == POS_LONG:
            # Прямой способ (когда z_score входит в диапазон выхода)
            if not reverse_out:
                if z > thresh_high_out:
                    signal = SIG_LONG_CLOSE
                    reason = REASON_THRESHOLD
            # Выходим из сделки, когда z_score покидает диапазон выхода
            else:
                if not long_flag_out and z > thresh_high_out:
                    long_flag_out = 1
                elif long_flag_out and z < thresh_high_out:
                    signal = SIG_LONG_CLOSE
                    reason = REASON_THRESHOLD
                    long_flag_out = 0

        # --- Проверяем стоп-лосс ---
        if z > stop_loss_std and pos_side == POS_SHORT:
            signal = SIG_SHORT_CLOSE
            reason = REASON_STOPLOSS
            if sl_method == 2:
                sl_block_short = 1
        elif z < -stop_loss_std and pos_side == POS_LONG:
            signal = SIG_LONG_CLOSE
            reason = REASON_STOPLOSS
            if sl_method == 2:
                sl_block_long = 1

        if reason == REASON_STOPLOSS and sl_method == 1 and sl_seconds > 0:
            sl_counter = sl_seconds

    return out, events

def backtest(df, token_1, token_2, dp_1, dp_2, ps_1, ps_2, thresh_low_in, thresh_low_out,
             thresh_high_in, thresh_high_out, long_possible, short_possible,
             balance, order_size, fee_rate,  stop_loss_std, sl_method=None,
             sl_seconds=0, leverage=1,
             method_in='direct', method_out='direct',
             verbose=False):
    assert method_in in ('direct', 'reverse')
    assert method_out in ('direct', 'reverse')

    time_arr = df['ts'].to_numpy()
    z = df["z_score"].to_numpy()
    bid_1 = df[f"{token_1}_bid_price"].to_numpy()
    ask_1 = df[f"{token_1}_ask_price"].to_numpy()
    bid_2 = df[f"{token_2}_bid_price"].to_numpy()
    ask_2 = df[f"{token_2}_ask_price"].to_numpy()

    reverse_in = method_in == 'reverse'
    reverse_out = method_out == 'reverse'

    sl_map = {None: 0, 'counter': 1, 'leave': 2}

    res, events = backtest_fast(time_arr, z, bid_1, ask_1, bid_2, ask_2,
            dp_1, dp_2, ps_1, ps_2,
            thresh_low_in=thresh_low_in, thresh_high_in=thresh_high_in,
            thresh_low_out=thresh_low_out, thresh_high_out=thresh_high_out,
            long_possible=long_possible, short_possible=short_possible,
            reverse_in=reverse_in, reverse_out=reverse_out,
            balance=balance, order_size=order_size, fee_rate=fee_rate,
            stop_loss_std=stop_loss_std, sl_method=sl_map[sl_method],
            sl_seconds=sl_seconds,
            leverage=leverage)

    trades_df = pl.DataFrame(res, schema=[
            "open_ts", "close_ts", "qty_1", "qty_2", "open_price_1", "close_price_1",
            "open_price_2", "close_price_2", "pos_side", "fees", "profit_1", "profit_2",
            "total_profit", "reason"], orient="row")

    if trades_df.height > 0:
        profit = trades_df['total_profit'].sum()
    else:
        profit = 0

    if verbose >= 1:
        print(f'low_in: {thresh_low_in}; high_in: {thresh_high_in}; \
low_out: {thresh_low_out}; high_out: {thresh_high_out}. n_trades: {trades_df.height}.\
Profit: {profit:.2f}.')
    if verbose >= 2:
        for ev in events:
            etype, open_time, close_time, qty_1, qty_2, open_price_1, price_1, open_price_2, price_2, pos_side, fees, profit_1, profit_2, total_profit, reason = ev

            open_date = datetime.fromtimestamp(open_time).strftime('%Y-%m-%d %H:%M:%S')
            close_date = datetime.fromtimestamp(close_time).strftime('%Y-%m-%d %H:%M:%S')

            side_1 = 'Buy' if pos_side == 1 else 'Sell'
            side_2 = 'Buy' if pos_side == 2 else 'Sell'

            if dp_1 >= 1:
                qty_1 = int(qty_1)
            if dp_2 >= 1:
                qty_2 = int(qty_2)

            if etype == 1:
                print(f"[ Open] {open_date}. {side_1} {qty_1} {token_1}, {side_2} {qty_2} {token_2}")
            elif etype == 2:
                print(f"[Close] {close_date}. Profit: {total_profit:.2f}")
            elif etype == 3:
                print(f"[STOP LOSS!] {close_date}. Profit: {total_profit:.2f}")
            elif etype == 4:
                print(f"[LIQUIDATION!] {close_date}. Profit: {total_profit:.2f}")

    return trades_df
