import polars as pl
import numpy as np
from datetime import datetime, timedelta

def get_duration_string(dur: int):
    if dur < 60:
        return f'{dur} seconds'
    elif dur < 3600:
        mins = dur // 60
        secs = dur - mins * 60
        return f'{mins} minutes {secs} seconds'
    elif dur < 3600 * 24:
        hours = dur // 3600
        mins = (dur - hours * 3600) // 60
        secs = dur - hours * 3600 - 60 * mins
        return f'{hours} hours {mins} minutes {secs} seconds'

def analyze_strategy(df: pl.DataFrame, start_date, end_date,
                     initial_balance: float = 1000.0) -> dict:
    """
    Анализирует торговую стратегию на основе данных о сделках и возвращает ключевые метрики.

    Args:
    -------
    trades_df (pl.DataFrame): DataFrame с историей торговых сделок, содержащий колонки:
        - open_ts (i64): Unix timestamp открытия сделки
        - close_ts (i64): Unix timestamp закрытия сделки
        - total_profit (f64): Суммарная прибыль/убыток сделки в USDT
        - fees (f64): Комиссии за сделку
        - reason (i64): Причина закрытия (1 - z_score, 2 - стоп-лосс, 3 - ликвидация)
        - и другие колонки (qty_1, qty_2, prices, etc.)

    initial_balance (float): Начальный баланс счета в USDT
    order_size (float): Размер ордера в USDT

    Return:
    -------
    dict: Словарь с рассчитанными метриками стратегии
    """
    if df is None or df.height == 0:
        return {}

    metrics = dict()
    df = df.sort(by="open_ts")

    # --- Базовая информация ---
    total_seconds = (end_date - start_date).total_seconds()

    metrics['total_days'] = round(total_seconds / 86400.0, 2)
    metrics['n_trades'] = len(df)

    # --- Рассчитываем длительности сделок ---
    df = df.with_columns((pl.col("close_ts") - pl.col("open_ts")).alias('duration'))

    min_dur = int(df['duration'].min())
    max_dur = int(df['duration'].max())
    avg_dur = int(df['duration'].mean())

    metrics["duration_min"] = get_duration_string(min_dur)
    metrics["duration_max"] = get_duration_string(max_dur)
    metrics["duration_avg"] = get_duration_string(avg_dur)
    metrics['time_in_trade'] = round(df['duration'].sum() / total_seconds, 2)

    # --- Проверяем наличие стоп-лоссов и ликвидаций ---
    metrics['stop_losses'] = df.filter(pl.col("reason") == 2).height
    metrics['liquidations'] = df.filter(pl.col("reason") == 3).height

    # --- Расчет баланса по истории сделок ---
    df = df.with_columns(
        pl.col("total_profit").cum_sum().alias("cum_profit")
    ).with_columns(
        (pl.lit(float(initial_balance)) + pl.col("cum_profit")).alias("balance")
    )

    metrics['initial_balance'] = initial_balance
    metrics['final_balance'] = round(df['balance'][-1], 4)

    # --- Доходность ---
    metrics["total_perc_return"] = round((metrics['final_balance']
                                / metrics['initial_balance'] - 1) * 100, 2)

    # try:
    #     metrics["annual_return"] = round(((metrics['final_balance'] / metrics['initial_balance']
    #                                       ) ** (365 / metrics['total_days']) - 1) * 100
    #                                       , 2)
    # except TypeError:
    #     metrics["annual_return"] = -100
    # except ZeroDivisionError:
    #     metrics["annual_return"] = 0
    #     return metrics

    # --- Рассчет изменений баланса ---
    # df = df.with_columns(
    #     (pl.col("balance") - pl.col("balance").shift(1)).alias("absolute_change"),
    #     (((pl.col("balance") / pl.col("balance").shift(1)) - 1) * 100).alias("percent_change")
    # )

    # --- Показатели просадки (Drawdown) ---
    df = df.with_columns(
        pl.col("balance").cum_max().alias("cum_max")
    )
    df = df.with_columns(
        (pl.col("balance") - pl.col("cum_max")).alias("drawdown") # В абсолютных величинах
    )

    metrics['max_drawdown'] = round(
        df.select("drawdown",'cum_profit').min_horizontal().min(),
        2)

    # --- Информация по сделкам ---
    metrics['max_profit'] = round(df['total_profit'].max(), 2)
    metrics['max_loss'] = round(min(df['total_profit'].min(), 0), 2)
    metrics['avg_profit'] = round(df['total_profit'].mean(), 2)


    profit_ratio = round(df['total_profit'].sum() / (abs(metrics['max_drawdown']) + 1), 3)

    metrics['profit_ratio'] = profit_ratio


    return metrics

    # Коэффициент Шарпа
    metrics['sharpe_ratio'] = round((metrics['avg_return'] / metrics['std_return'])
                                    * np.sqrt(metrics['trades_per_year']), 4)

    # Коэффициент Сортино
    downside_returns = df.filter(pl.col("percent_change") < 0)["percent_change"]

    if downside_returns.len() > 0:
        downside_std = downside_returns.std()
        metrics['sortino_ratio'] = round(
            (metrics['avg_return'] / downside_std) * np.sqrt(metrics['trades_per_year']),
            4
        ) if downside_std != 0 else float('nan')
    else:
        metrics['sortino_ratio'] = float('nan')

    # Коэффициент Кальмара: отношение CAGR к абсолютной величине максимальной просадки
    if isinstance(metrics['max_drawdown'], float) and metrics['max_drawdown'] < 1:
        metrics['calmar_ratio'] = round(metrics["annual_return"] / abs(metrics['max_drawdown'])
                                        if metrics['max_drawdown'] != 0 else np.nan, 4)
    elif metrics['max_drawdown'] > 1:
        raise Exception('Величина max_drawdown не может быть больше 1!')

    # --- Аналитика по сделкам ---
    metrics['win_ratio'] = round(metrics['winning_trades'] / metrics['n_trades']
                                 if metrics['n_trades'] > 0 else 0, 2)

    return metrics
