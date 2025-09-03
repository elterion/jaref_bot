import polars as pl
import polars_ols as pls
import numpy as np
from numba import njit, prange

def make_trunc_df(df, timeframe, token_1, token_2, start_date=None, end_date=None, method="last"):
    select_spread = True if 'spread' in df.columns else False

    df = df.with_columns(
            ((pl.col(f'{token_1}_bid_price') + pl.col(f'{token_1}_ask_price')) / 2).alias(token_1),
            ((pl.col(f'{token_2}_bid_price') + pl.col(f'{token_2}_ask_price')) / 2).alias(token_2),
        )
    if select_spread:
        df = df.select('time', 'ts', token_1, token_2, 'spread')
    else:
        df = df.select('time', 'ts', token_1, token_2)

    # условия агрегации
    if method == "last":
        agg_exprs = [
            pl.col("ts").first(),
            pl.col(token_1).last().alias(token_1),
            pl.col(token_2).last().alias(token_2),
        ]

        if select_spread:
            agg_exprs.append(pl.col("spread").last().alias("spread"))

    elif method == "triple":
        agg_exprs = [
            pl.col("ts").first(),
            ((pl.col(token_1).last() + pl.col(token_1).max() + pl.col(token_1).min()) / 3).alias(token_1),
            ((pl.col(token_2).last() + pl.col(token_2).max() + pl.col(token_2).min()) / 3).alias(token_2),
        ]

        if select_spread:
            agg_exprs.append(((pl.col("spread").last() + pl.col("spread").max() +
                               pl.col("spread").min()) / 3).alias("spread"))
    else:
        raise ValueError(f"Unknown method: {method}")

    df = df.group_by_dynamic(
                index_column="time",
                every=timeframe,
                label='left'
            ).agg(agg_exprs)
    if start_date:
        df = df.filter(pl.col('time') > start_date)
    if end_date:
        df = df.filter(pl.col('time') < end_date)

    return df

def make_df_from_orderbooks(df_1, df_2, token_1, token_2,
                            start_time=None, end_time=None,
                            return_spread=True, log_spread=True):
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
        spread = df['spread'].to_numpy()
        means, stds, z_scores = get_dist_zscore(spread, winds_np)
        return means, stds, z_scores

@njit(fastmath=True)
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

@njit(fastmath=True)
def get_dist_zscore(spread: np.ndarray, winds: np.ndarray):
    """
    spread: 1D float64 array (n,)
    winds: 1D int64 array (m,)
    returns: means (m,n), stds (m,n), zs (m,n)
    """
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
