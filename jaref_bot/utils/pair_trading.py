import polars as pl
import polars_ols as pls
import numpy as np
from numba import njit
from jaref_bot.utils.coins import get_step_info
import math
import ast

def round_down(value: float, dp: float):
    return round(math.floor(value / dp) * dp, 6)

def make_trunc_df(df, timeframe, token_1, token_2, start_date=None, end_date=None,
                  method="last", offset='0h', return_bid_ask=False):
    select_spread = True if 'spread' in df.columns else False

    df = df.with_columns(
            ((pl.col(f'{token_1}_bid_price') + pl.col(f'{token_1}_ask_price')) / 2).alias(token_1),
            ((pl.col(f'{token_2}_bid_price') + pl.col(f'{token_2}_ask_price')) / 2).alias(token_2),
            pl.min_horizontal(f'{token_1}_bid_size', f'{token_1}_ask_size').alias(f'{token_1}_size'),
            pl.min_horizontal(f'{token_2}_bid_size', f'{token_2}_ask_size').alias(f'{token_2}_size')
        )

    # условия агрегации
    if method == "last":
        agg_exprs = [
            pl.col("ts").first(),
            pl.col(token_1).last().alias(token_1),
            pl.col(token_2).last().alias(token_2),
            pl.col(f'{token_1}_size').sum(),
            pl.col(f'{token_2}_size').sum(),
        ]

        if select_spread:
            agg_exprs.append(pl.col("spread").last().alias("spread"))

    elif method == "triple":
        agg_exprs = [
            pl.col("ts").first(),
            ((pl.col(token_1).last() + pl.col(token_1).max() + pl.col(token_1).min()) / 3).alias(token_1),
            ((pl.col(token_2).last() + pl.col(token_2).max() + pl.col(token_2).min()) / 3).alias(token_2),
            pl.col(f'{token_1}_size').sum(),
            pl.col(f'{token_2}_size').sum(),
        ]

        if select_spread:
            agg_exprs.append(((pl.col("spread").last() + pl.col("spread").max() +
                               pl.col("spread").min()) / 3).alias("spread"))
    else:
        raise ValueError(f"Unknown method: {method}")

    if return_bid_ask:
        agg_exprs.extend((
            pl.col(f'{token_1}_bid_price').last(),
            pl.col(f'{token_1}_ask_price').last(),
            pl.col(f'{token_2}_bid_price').last(),
            pl.col(f'{token_2}_ask_price').last(),
            pl.col(f'{token_1}_bid_size').last(),
            pl.col(f'{token_1}_ask_size').last(),
            pl.col(f'{token_2}_bid_size').last(),
            pl.col(f'{token_2}_ask_size').last()
        ))

    df = df.group_by_dynamic(
                index_column="time",
                every=timeframe,
                offset=offset,
                label='left'
            ).agg(agg_exprs)
    if start_date:
        df = df.filter(pl.col('time') > start_date)
    if end_date:
        df = df.filter(pl.col('time') < end_date)

    return df

def make_df_from_orderbooks(df_1, df_2, token_1, token_2,
                            start_time=None, end_time=None,
                            return_spread=False, log_spread=False):
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

    if return_spread:
        if log_spread:
            df = df.with_columns(
                (pl.col(token_1).log() - pl.col(token_2).log()).alias('spread')
            )
        else:
            df = df.with_columns(
                (pl.col(token_1) - pl.col(token_2)).alias('spread')
            )

    return df

def make_zscore_df(df, token_1, token_2, wind, method='dist'):
    if method == 'dist':
        return df.lazy().with_columns(
                (pl.col(token_1).log() - pl.col(token_2).log()).alias('spread')
            ).with_columns(
                pl.col('spread').rolling_mean(wind).alias(f'mean'),
                pl.col('spread').rolling_std(wind).alias(f'std')
            ).with_columns(
                ((pl.col('spread') - pl.col('mean')) / pl.col('std')).alias(f'z_score')
            ).collect()
    elif method == 'lr':
        return df.lazy().with_columns(
            pl.col(token_1)
            .least_squares.rolling_ols(pl.col(token_2),
                                    mode='coefficients',
                                    add_intercept=True,
                                    window_size=wind)
            .alias("predictions")
        ).rename({token_2: 'temp'}
        ).unnest('predictions'
        ).rename({token_2: 'beta'}
        ).rename({'temp': token_2}
        ).with_columns(
            (pl.col(token_1) - pl.col('const') - pl.col('beta') * pl.col(token_2)).alias('spread')
        ).with_columns(
            pl.col('spread').rolling_mean(wind).alias('mean'),
            pl.col('spread').rolling_std(wind).alias('std')
        ).with_columns(
            ((pl.col('spread') - pl.col('mean')) / pl.col('std')).alias('z_score')
        ).collect()

def get_zscore(df, token_1, token_2, winds, method):
    t1 = df[token_1].to_numpy()
    t2 = df[token_2].to_numpy()
    winds_np = np.array(winds)

    if method == 'lr':
        alpha, beta, zscore = get_lr_zscore(t1, t2, winds_np)
        return alpha, beta, zscore
    elif method == 'dist':
        means, stds, z_scores = get_dist_zscore(t1, t2, winds_np)
        return means, stds, z_scores

@njit
def get_lr_zscore(t1, t2, winds):
    """
    t1, t2: 1D np.ndarray(float64) длины n
    winds: 1D np.ndarray(int64) длины m (несортированный порядок сохраняется)
    Возвращает:
      alpha_full, beta_full, z_full - массивы формы (m, n)
    """
    n = t1.shape[0]
    m = winds.shape[0]

    alpha_full = np.full((m, n), np.nan, dtype=np.float64)
    beta_full  = np.full((m, n), np.nan, dtype=np.float64)
    z_full     = np.full((m, n), np.nan, dtype=np.float64)

    max_w = 0
    for j in range(m):
        if winds[j] > max_w:
            max_w = winds[j]
    if max_w <= 0:
        return alpha_full[:, -1], beta_full[:, -1], z_full[:, -1]

    spread_bufs = np.zeros((m, max_w), dtype=np.float64)

    sum_x  = np.zeros(m, dtype=np.float64)
    sum_y  = np.zeros(m, dtype=np.float64)
    sum_xx = np.zeros(m, dtype=np.float64)
    sum_xy = np.zeros(m, dtype=np.float64)

    sum_s  = np.zeros(m, dtype=np.float64)
    sum_ss = np.zeros(m, dtype=np.float64)

    for i in range(n):
        x = t2[i]
        y = t1[i]

        for j in range(m):
            w = winds[j]
            if w <= 0:
                continue

            sum_x[j] += x
            sum_y[j] += y
            sum_xx[j] += x * x
            sum_xy[j] += x * y

            if i >= w:
                x_old = t2[i - w]
                y_old = t1[i - w]
                sum_x[j]  -= x_old
                sum_y[j]  -= y_old
                sum_xx[j] -= x_old * x_old
                sum_xy[j] -= x_old * y_old

            if i >= w - 1:
                mean_x = sum_x[j] / w
                mean_y = sum_y[j] / w

                var_x = (sum_xx[j] / w) - mean_x * mean_x
                cov_xy = (sum_xy[j] / w) - mean_x * mean_y

                if var_x <= 0.0 or not np.isfinite(var_x):
                    beta = np.nan
                    alpha = np.nan
                else:
                    beta = cov_xy / var_x
                    alpha = mean_y - beta * mean_x

                beta_full[j, i] = beta
                alpha_full[j, i] = alpha

                s = y - (alpha + beta * x)

                pos = i % w
                if i >= w:
                    s_old = spread_bufs[j, pos]
                    sum_s[j]  -= s_old
                    sum_ss[j] -= s_old * s_old

                spread_bufs[j, pos] = s
                sum_s[j]  += s
                sum_ss[j] += s * s

                if w > 1:
                    mean_s = sum_s[j] / w
                    num = sum_ss[j] - w * mean_s * mean_s
                    denom = w - 1
                    if num > 0.0 and np.isfinite(num):
                        var_s_sample = num / denom
                        z = (s - mean_s) / np.sqrt(var_s_sample)
                        z_full[j, i] = z
                    else:
                        z_full[j, i] = np.nan
                else:
                    z_full[j, i] = np.nan

            else:
                pass

    return alpha_full[:, -1], beta_full[:, -1], z_full[:, -1]

@njit
def get_dist_zscore(t1: np.ndarray, t2: np.ndarray, winds: np.ndarray):
    """
    spread: 1D float64 array (n,)
    winds: 1D int64 array (m,)
    returns: means (m,n), stds (m,n), zs (m,n)
    """
    spread = np.log(t1) - np.log(t2)

    n = spread.shape[0]
    m = winds.shape[0]

    # Prepare outputs
    means = np.full((m, n), np.nan, dtype=np.float64)
    stds = np.full((m, n), np.nan, dtype=np.float64)
    zs = np.full((m, n), np.nan, dtype=np.float64)

    if n == 0 or m == 0:
        return means[:, -1], stds[:, -1], zs[:, -1]

    # Вычисляем кумулятивные суммы и квадраты (внутри компилированной функции)
    cumulative = np.empty(n, dtype=np.float64)
    cumulative_sq = np.empty(n, dtype=np.float64)
    s = 0.0
    sq = 0.0
    for i in range(n):
        s += spread[i]
        sq += spread[i] * spread[i]
        cumulative[i] = s
        cumulative_sq[i] = sq

    # Распараллеливание по окнам (каждое окно — независимая задача)
    for wi in range(m):
        wind = winds[wi]
        if wind <= 1 or n < wind:
            # оставляем строки заполненными NaN
            continue

        # для каждого окна двигаемся по позициям
        for i in range(wind - 1, n):
            start_index = i - wind + 1
            if start_index == 0:
                s_window = cumulative[i]
                sq_window = cumulative_sq[i]
            else:
                s_window = cumulative[i] - cumulative[start_index - 1]
                sq_window = cumulative_sq[i] - cumulative_sq[start_index - 1]

            mean_val = s_window / wind
            # ddof=1
            variance = (sq_window - wind * mean_val * mean_val) / (wind - 1)
            if variance < 0.0:
                # численные погрешности
                variance = 0.0
            std_val = np.sqrt(variance)

            means[wi, i] = mean_val
            stds[wi, i] = std_val
            if std_val != 0.0:
                zs[wi, i] = (spread[i] - mean_val) / std_val
            else:
                zs[wi, i] = np.nan

    return means[:, -1], stds[:, -1], zs[:, -1]

@njit
def binary_search_left(arr, x):
    """Найти индекс первой позиции в arr, где arr[idx] >= x.
       arr отсортирован по возрастанию."""
    lo = 0
    hi = arr.shape[0]
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


@njit
def create_2df_loop(
    nrows,
    sec_ts, sec_t1, sec_t2, sec_s1, sec_s2,
    hour4_ts, hour4_t1, hour4_t2, max_hour4_wind,
    hour1_ts, hour1_t1, hour1_t2, max_hour1_wind,
    min_order,
    hour4_winds, hour1_winds,
    ts_buf, t1_buf, t2_buf, s1_buf, s2_buf,
    beta4_buf, z4_buf, beta1_buf, z1_buf
):
    pos = 0
    # временные массивы (максимальный размер для окон + 1)
    tmp_win4_t1 = np.empty(max_hour4_wind + 1, dtype=np.float64)
    tmp_win4_t2 = np.empty(max_hour4_wind + 1, dtype=np.float64)
    tmp_win1_t1 = np.empty(max_hour1_wind + 1, dtype=np.float64)
    tmp_win1_t2 = np.empty(max_hour1_wind + 1, dtype=np.float64)
    tmp_last6_t1 = np.empty(6, dtype=np.float64)
    tmp_last6_t2 = np.empty(6, dtype=np.float64)

    for i in range(nrows):
        # 1) базовые поля из sec arrays
        cur_ts = sec_ts[i]
        cur_t1 = sec_t1[i]
        cur_t2 = sec_t2[i]
        cur_s1 = sec_s1[i]
        cur_s2 = sec_s2[i]

        # 2) последние 6 строк и медиана (фильтр по объёму)
        start6 = i - 6
        if start6 < 0:
            start6 = 0
        end6 = i
        sel_cnt = 0
        for j in range(start6, end6):
            vol1 = sec_s1[j] * sec_t1[j]
            vol2 = sec_s2[j] * sec_t2[j]
            if (vol1 > min_order) and (vol2 > min_order):
                tmp_last6_t1[sel_cnt] = sec_t1[j]
                tmp_last6_t2[sel_cnt] = sec_t2[j]
                sel_cnt += 1

        if sel_cnt == 0:
            t1_med = cur_t1
            t2_med = cur_t2
        else:
            t1_med = np.median(tmp_last6_t1)
            t2_med = np.median(tmp_last6_t2)

        # 3) hour4_stat: взять записи hour4_ts < cur_ts и хвост длины max_hour4_wind
        idx4 = binary_search_left(hour4_ts, cur_ts)
        start4 = idx4 - max_hour4_wind
        if start4 < 0:
            start4 = 0
        len4 = idx4 - start4
        for j in range(len4):
            tmp_win4_t1[j] = hour4_t1[start4 + j]
            tmp_win4_t2[j] = hour4_t2[start4 + j]
        tmp_win4_t1[len4] = t1_med
        tmp_win4_t2[len4] = t2_med
        total4_len = len4 + 1

        # 4) hour1_stat аналогично
        idx1 = binary_search_left(hour1_ts, cur_ts)
        start1 = idx1 - max_hour1_wind
        if start1 < 0:
            start1 = 0
        len1 = idx1 - start1
        for j in range(len1):
            tmp_win1_t1[j] = hour1_t1[start1 + j]
            tmp_win1_t2[j] = hour1_t2[start1 + j]
        tmp_win1_t1[len1] = t1_med
        tmp_win1_t2[len1] = t2_med
        total1_len = len1 + 1

        # Создаём короткие массивы (копируем) — это numba-совместимо
        small4_t1 = np.empty(total4_len, dtype=np.float64)
        small4_t2 = np.empty(total4_len, dtype=np.float64)
        for j in range(total4_len):
            small4_t1[j] = tmp_win4_t1[j]
            small4_t2[j] = tmp_win4_t2[j]

        small1_t1 = np.empty(total1_len, dtype=np.float64)
        small1_t2 = np.empty(total1_len, dtype=np.float64)
        for j in range(total1_len):
            small1_t1[j] = tmp_win1_t1[j]
            small1_t2[j] = tmp_win1_t2[j]

        alpha4, beta4_vals, z4_vals = get_lr_zscore(small4_t1, small4_t2, hour4_winds)
        alpha1, beta1_vals, z1_vals = get_lr_zscore(small1_t1, small1_t2, hour1_winds)

        # 6) Записываем все в буферы
        ts_buf[pos] = cur_ts
        t1_buf[pos] = cur_t1
        t2_buf[pos] = cur_t2
        s1_buf[pos] = cur_s1
        s2_buf[pos] = cur_s2

        # beta4/z4 для всех окон
        for k in range(beta4_vals.shape[0]):
            beta4_buf[pos, k] = beta4_vals[k]
            z4_buf[pos, k] = z4_vals[k]
        for k in range(beta1_vals.shape[0]):
            beta1_buf[pos, k] = beta1_vals[k]
            z1_buf[pos, k] = z1_vals[k]

        pos += 1

    return pos

@njit
def create_1df_loop(
    nrows,
    sec_ts, sec_t1, sec_t2, sec_s1, sec_s2,
    hour1_ts, hour1_t1, hour1_t2, max_hour1_wind,
    min_order, hour1_winds,
    ts_buf, t1_buf, t2_buf, s1_buf, s2_buf,
    beta1_buf, z1_buf
):
    pos = 0
    # временные массивы (максимальный размер для окон + 1)
    tmp_win1_t1 = np.empty(max_hour1_wind + 1, dtype=np.float64)
    tmp_win1_t2 = np.empty(max_hour1_wind + 1, dtype=np.float64)
    tmp_last6_t1 = np.empty(6, dtype=np.float64)
    tmp_last6_t2 = np.empty(6, dtype=np.float64)

    for i in range(nrows):
        # 1) базовые поля из sec arrays
        cur_ts = sec_ts[i]
        cur_t1 = sec_t1[i]
        cur_t2 = sec_t2[i]
        cur_s1 = sec_s1[i]
        cur_s2 = sec_s2[i]

        # 2) последние 6 строк и медиана (фильтр по объёму)
        start6 = i - 6
        if start6 < 0:
            start6 = 0
        end6 = i
        sel_cnt = 0
        for j in range(start6, end6):
            vol1 = sec_s1[j] * sec_t1[j]
            vol2 = sec_s2[j] * sec_t2[j]
            if (vol1 > min_order) and (vol2 > min_order):
                tmp_last6_t1[sel_cnt] = sec_t1[j]
                tmp_last6_t2[sel_cnt] = sec_t2[j]
                sel_cnt += 1

        if sel_cnt == 0:
            t1_med = cur_t1
            t2_med = cur_t2
        else:
            t1_med = np.median(tmp_last6_t1)
            t2_med = np.median(tmp_last6_t2)

        # 4) hour1_stat аналогично
        idx1 = binary_search_left(hour1_ts, cur_ts)
        start1 = idx1 - max_hour1_wind
        if start1 < 0:
            start1 = 0
        len1 = idx1 - start1
        for j in range(len1):
            tmp_win1_t1[j] = hour1_t1[start1 + j]
            tmp_win1_t2[j] = hour1_t2[start1 + j]
        tmp_win1_t1[len1] = t1_med
        tmp_win1_t2[len1] = t2_med
        total1_len = len1 + 1

        small1_t1 = np.empty(total1_len, dtype=np.float64)
        small1_t2 = np.empty(total1_len, dtype=np.float64)
        for j in range(total1_len):
            small1_t1[j] = tmp_win1_t1[j]
            small1_t2[j] = tmp_win1_t2[j]

        alpha1, beta1_vals, z1_vals = get_lr_zscore(small1_t1, small1_t2, hour1_winds)

        # 6) Записываем все в буферы
        ts_buf[pos] = cur_ts
        t1_buf[pos] = cur_t1
        t2_buf[pos] = cur_t2
        s1_buf[pos] = cur_s1
        s2_buf[pos] = cur_s2

        # beta4/z4 для всех окон
        for k in range(beta1_vals.shape[0]):
            beta1_buf[pos, k] = beta1_vals[k]
            z1_buf[pos, k] = z1_vals[k]
        pos += 1

    return pos

def create_zscore_2df(token_1, token_2, df_sec, df_4hour, df_hour,
              hour4_winds, hour1_winds, spread_method, min_order):

    method_is_lr = 1 if spread_method == 'lr' else 0

    max_hour1_wind = 2 * int(hour1_winds.max())
    max_hour4_wind = 2 * int(hour4_winds.max())

    # --- Перевод polars в numpy ---
    tss = df_sec['ts'].to_numpy()
    times = df_sec['time'].to_numpy()
    size1 = df_sec[f'{token_1}_size'].to_numpy()   # np.ndarray, shape (n,)
    price1 = df_sec[token_1].to_numpy()
    size2 = df_sec[f'{token_2}_size'].to_numpy()
    price2 = df_sec[token_2].to_numpy()
    bp1 = df_sec[f'{token_1}_bid_price'].to_numpy()
    bp2 = df_sec[f'{token_2}_bid_price'].to_numpy()
    ap1 = df_sec[f'{token_1}_ask_price'].to_numpy()
    ap2 = df_sec[f'{token_2}_ask_price'].to_numpy()

    bs1 = df_sec[f'{token_1}_bid_size'].to_numpy()
    bs2 = df_sec[f'{token_2}_bid_size'].to_numpy()
    as1 = df_sec[f'{token_1}_ask_size'].to_numpy()
    as2 = df_sec[f'{token_2}_ask_size'].to_numpy()

    n = tss.shape[0]

    # df_4hour arrays (предполагается что df_4hour отсортирован по ts по возрастанию)
    hour4_ts = df_4hour['ts'].to_numpy()
    hour4_t1 = df_4hour[token_1].to_numpy()
    hour4_t2 = df_4hour[token_2].to_numpy()
    # (sizes если нужно — добавьте аналогично)

    # df_hour arrays
    hour1_ts = df_hour['ts'].to_numpy()
    hour1_t1 = df_hour[token_1].to_numpy()
    hour1_t2 = df_hour[token_2].to_numpy()

    # --- Предвыделение буферов для rows_buffer ---
    ts_buf = np.empty(n, dtype=np.int64)
    t1_buf = np.empty(n, dtype=np.float64)
    t2_buf = np.empty(n, dtype=np.float64)
    s1_buf = np.empty(n, dtype=np.float64)
    s2_buf = np.empty(n, dtype=np.float64)

    # --- Кол-во окон ---
    m4 = hour4_winds.shape[0]
    m1 = hour1_winds.shape[0]

    # --- 2D буферы: (rows, num_windows) ---
    beta4_buf = np.full((n, m4), np.nan, dtype=np.float64)
    z4_buf = np.full((n, m4), np.nan, dtype=np.float64)
    beta1_buf = np.full((n, m1), np.nan, dtype=np.float64)
    z1_buf = np.full((n, m1), np.nan, dtype=np.float64)

    pos = create_2df_loop(
        n,
        tss, price1, price2, size1, size2,
        hour4_ts, hour4_t1, hour4_t2, max_hour4_wind,
        hour1_ts, hour1_t1, hour1_t2, max_hour1_wind,
        min_order,
        hour4_winds, hour1_winds,
        ts_buf, t1_buf, t2_buf, s1_buf, s2_buf,
        beta4_buf, z4_buf, beta1_buf, z1_buf
    )

    # --- Собираем итоговый polars DataFrame из буферов (только заполненные строки) ---
    out = {
        'time': times[:pos],
        'ts': ts_buf[:pos],
        token_1: t1_buf[:pos],
        token_2: t2_buf[:pos],
        f'{token_1}_size': s1_buf[:pos],
        f'{token_2}_size': s2_buf[:pos],
        f'{token_1}_bid_price': bp1[:pos],
        f'{token_2}_bid_price': bp2[:pos],
        f'{token_1}_ask_price': ap1[:pos],
        f'{token_2}_ask_price': ap2[:pos],
        f'{token_1}_bid_size': bs1[:pos],
        f'{token_2}_bid_size': bs2[:pos],
        f'{token_1}_ask_size': as1[:pos],
        f'{token_2}_ask_size': as2[:pos]
    }

    for i in range(m4):
        w = int(hour4_winds[i])
        out[f'beta_{w}_4h'] = beta4_buf[:pos, i]
        out[f'z_score_{w}_4h'] = z4_buf[:pos, i]
    for i in range(m1):
        w = int(hour1_winds[i])
        out[f'beta_{w}_1h'] = beta1_buf[:pos, i]
        out[f'z_score_{w}_1h'] = z1_buf[:pos, i]

    return pl.DataFrame(out, infer_schema_length=None).drop_nans(
                ).with_columns(
                    pl.col('time').dt.convert_time_zone('Europe/Moscow')
                )

@njit
def create_zscore_curve(start_ts: int,
                        median_length: int,
                        tss: np.ndarray,
                        price1: np.ndarray,
                        price2: np.ndarray,
                        size1: np.ndarray,
                        size2: np.ndarray,
                        hist_ts: np.ndarray,
                        hist_t1: np.ndarray,
                        hist_t2: np.ndarray,
                        winds: np.ndarray,
                        min_order: float):
    """
    Функция на исторических данных рассчитывает z_score для каждой строки.

    Args:
        start_ts: unix timestamp начала бектеста
        median_length: количество секунд для вычисления медианы
        tss: массив, состоящий из unix timestamp, для каждой строки секундного датафрейма
        price1: массив из цен для токена_1
        price2: массив из цен для токена_2
        size1: массив из доступных объёмов для токена_1
        size2: массив из доступных объёмов для токена_2
        hist_ts: массив из unix timestamp для каждой строки датафрейма с агрегированными данными
        hist_t1: массив из цен для токена_1 из датафрейма с агрегированными данными
        hist_t2: массив из цен для токена_2 из датафрейма с агрегированными данными
        winds: массив из размеров окон
        min_order: размер минимального ордера в usdt для фильтрации слишком малого объёма

    """
    nrows = tss.shape[0]
    ts_arr = np.full(nrows, np.nan)
    z_score_arr = np.full(nrows, np.nan)

    for i in range(nrows):
        # Пропускаем начало датафрейма, нужное для вычисления медианы
        if tss[i] <= start_ts:
            continue

        t1_price = price1[i - median_length: i]
        t2_price = price2[i - median_length: i]
        t1_size = size1[i - median_length: i]
        t2_size = size2[i - median_length: i]

        if np.sum(t1_price * t1_size > min_order) < 3 or np.sum(t2_price * t2_size > min_order) < 3:
            continue

        t1_med = np.median(t1_price)
        t2_med = np.median(t2_price)

        # Выберем из агрегированных цен только те, которые были до текущего момента
        n_elems = hist_ts[hist_ts < tss[i]][:-1].shape[0]
        tail = hist_ts.shape[0] - n_elems
        t1_hist = hist_t1[:-tail]
        t2_hist = hist_t2[:-tail]

        # Сформируем массивы, в которых к историческим данным в конец добавим текущую медианную цену, и посчитаем z_score
        t1_arr_med = np.append(t1_hist, t1_med)
        t2_arr_med = np.append(t2_hist, t2_med)
        _, beta_med, zscore_med = get_lr_zscore(t1_arr_med, t2_arr_med, winds)

        ts_arr[i] = tss[i]
        z_score_arr[i] = zscore_med[0]

    return ts_arr[~np.isnan(ts_arr)].astype(np.int64), z_score_arr[~np.isnan(z_score_arr)]

def create_zscore_df(token_1, token_2, df_sec, agg_df, winds, min_order, start_ts, median_length):

    # method_is_lr = 1 if spread_method == 'lr' else 0

    # --- Перевод polars в numpy ---
    tss = df_sec['ts'].to_numpy()
    size1 = df_sec[f'{token_1}_size'].to_numpy()   # np.ndarray, shape (n,)
    price1 = df_sec[token_1].to_numpy()
    size2 = df_sec[f'{token_2}_size'].to_numpy()
    price2 = df_sec[token_2].to_numpy()

    hist_ts = agg_df['ts'].to_numpy()
    hist_t1 = agg_df[token_1].to_numpy()
    hist_t2 = agg_df[token_2].to_numpy()

    # --- Вычисляем z_score ---
    ts_arr, z_arr = create_zscore_curve(start_ts, median_length, tss, price1, price2, size1, size2,
                                        hist_ts, hist_t1, hist_t2, winds, min_order)

    # --- Собираем итоговый polars DataFrame из буферов (только заполненные строки) ---
    tdf = pl.DataFrame({'ts': ts_arr, 'z_score': z_arr})

    return df_sec.select('time', 'ts', token_1, token_2, f'{token_1}_size',
                         f'{token_2}_size', f'{token_1}_bid_price',
                         f'{token_1}_ask_price', f'{token_1}_bid_size',
                         f'{token_1}_ask_size', f'{token_2}_bid_price',
                         f'{token_2}_ask_price', f'{token_2}_bid_size',
                         f'{token_2}_ask_size').join(tdf, on='ts')

def get_qty(
        token_1: str,
        token_2: str,
        price_1: float,
        price_2: float,
        beta: float,
        coin_information: dict,
        total_usdt_amount: float = 100.0,
        fee_rate: float = 0.00055,
        method: 'str' = 'beta',
    ):
    """
        Вычисляет размеры позиций для двух активов.
        Args:
            price_1: цена актива 1 (в долларах)
            price_2: цена актива 2 (в долларах)
            beta: хедж-коэффициент (количество B на 1 A)
            coin_information: словарь с технической информацией по монетам
            total_usdt_amount: общий размер позиции в долларах
            fee_rate: комиссия за сделки
            method: метод распределения денег между плечами сделки. 'beta' или 'usdt_neutral'
        Returns:
            qty_1, qty_2: количество токенов 1 и 2
    """

    if not token_1.endswith('_USDT'):
        token_1 += '_USDT'
    if not token_2.endswith('_USDT'):
        token_2 += '_USDT'

    dp_1 = get_step_info(coin_information, token_1, 'bybit_linear', 'bybit_linear')
    dp_2 = get_step_info(coin_information, token_2, 'bybit_linear', 'bybit_linear')

    if method == 'beta':
        qty_1 = total_usdt_amount * (1 - fee_rate) / (price_1 + beta * price_2)
        qty_1 = round_down(qty_1, dp_1)
        qty_2 = beta * qty_1
        qty_2 = round_down(qty_2, dp_2)
    elif method == 'usdt_neutral':
        qty_1 = round_down(total_usdt_amount / 2 / (1.0 + 2.0 * fee_rate) / price_1, dp_1)
        qty_2 = round_down(total_usdt_amount / 2 / (1.0 + 2.0 * fee_rate) / price_2, dp_2)

    return qty_1, qty_2

def calculate_profit_curve(df, token_1, token_2, side, t1_op, t2_op, t1_qty, t2_qty, fee_rate):
    if side == 'long':
        tdf = df.select('time', f'{token_1}_bid_price', f'{token_1}_bid_size',
                        f'{token_2}_ask_price', f'{token_2}_ask_size', 'z_score').rename(
            {
                f'{token_1}_bid_price': f'{token_1}_price', f'{token_1}_bid_size': f'{token_1}_size',
                f'{token_2}_ask_price': f'{token_2}_price', f'{token_2}_ask_size': f'{token_2}_size',
            }
        )

        expr_t1_long = (
            pl.lit(t1_qty)
            * (pl.col(f"{token_1}_price") - pl.lit(t1_op) - pl.lit(fee_rate) * (pl.lit(t1_op) + pl.col(f"{token_1}_price")))
        )
        expr_t2_short = (
            pl.lit(t2_qty)
            * (pl.lit(t2_op) - pl.col(f"{token_2}_price") - pl.lit(fee_rate) * (pl.lit(t2_op) + pl.col(f"{token_2}_price")))
        )

        tdf = tdf.with_columns((expr_t1_long + expr_t2_short).alias("profit"))


    elif side == 'short':
        tdf = df.select('time', f'{token_1}_ask_price', f'{token_1}_ask_size',
                        f'{token_2}_bid_price', f'{token_2}_bid_size', 'z_score').rename(
            {
                f'{token_1}_ask_price': f'{token_1}_price', f'{token_1}_ask_size': f'{token_1}_size',
                f'{token_2}_bid_price': f'{token_2}_price', f'{token_2}_bid_size': f'{token_2}_size',
            }
        )

        expr_t1_short = (
            pl.lit(t1_qty)
            * (pl.lit(t1_op) - pl.col(f"{token_1}_price") - pl.lit(fee_rate) * (pl.lit(t1_op) + pl.col(f"{token_1}_price")))
        )

        expr_t2_long = (
            pl.lit(t2_qty)
            * (pl.col(f"{token_2}_price") - pl.lit(t2_op) - pl.lit(fee_rate) * (pl.lit(t2_op) + pl.col(f"{token_2}_price")))
        )

        tdf = tdf.with_columns((expr_t1_short + expr_t2_long).alias("profit"))

    return tdf

def get_thresholds():
    data = []
    with open('./jaref_bot/config/thresholds.txt', 'r') as file:
        for line in file:
            line = line.strip()  # Удаляем пробелы и переносы строк
            if line:  # Игнорируем пустые строки
                # Преобразуем строку в кортеж с помощью literal_eval
                tuple_data = ast.literal_eval(line)
                data.append(tuple_data)
    return data
