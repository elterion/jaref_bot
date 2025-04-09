import polars as pl
import numpy as np
from datetime import datetime

def make_1s_df(sym_1, sym_2, start_date, end_date):
    df_1 = pl.read_parquet(f'./data/{sym_1}_agg_trades.parquet')
    df_2 = pl.read_parquet(f'./data/{sym_2}_agg_trades.parquet')

    start = min(df_1["datetime"].min(), df_2["datetime"].min())
    end = max(df_1["datetime"].max(), df_2["datetime"].max())
    n_seconds = int((end - start).total_seconds()) + 1

    df_time = (
    pl.arange(0, n_seconds, eager=True)
    .alias("sec")
    .to_frame()
    .with_columns(
         (pl.lit(start) + (pl.col("sec") * 1000).cast(pl.Duration("ms"))).alias("datetime")
    )
    .select("datetime")
    ).join(df_1, on="datetime", how="left").join(df_2, on="datetime", how="left").filter(
        (pl.col("close").is_not_null()) |
        (pl.col("qty").is_not_null()) |
        (pl.col("close_right").is_not_null()) |
        (pl.col("qty_right").is_not_null())
    ).filter((pl.col('datetime') >= start_date) & (pl.col('datetime') < end_date)
    ).rename({'close': f'close_{sym_1}', 'qty': f'qty_{sym_1}', 'close_right': f'close_{sym_2}', 'qty_right': f'qty_{sym_2}'})

    for row in df_time.iter_rows():
        if row[1] and row[2] and row[3] and row[4]:
            first_point = row[0]
            break

    return df_time.filter(pl.col('datetime') >= first_point)

def make_stat_df(sym_1, sym_2, period, roll_wind, start_date, end_date):
    df_1 = pl.read_parquet(f'./data/{sym_1}_agg_trades.parquet')
    df_2 = pl.read_parquet(f'./data/{sym_2}_agg_trades.parquet')

    df_1 = df_1.group_by(pl.col("datetime").dt.truncate(period)
    ).agg([
              pl.col("close").median().alias('price'),
              pl.col("qty").sum()
          ]
    ).sort(by='datetime')

    df_2 = df_2.group_by(pl.col("datetime").dt.truncate(period)
    ).agg([
              pl.col("close").median().alias('price'),
              pl.col("qty").sum()
          ]
    ).sort(by='datetime')

    stat_df = df_1.join(df_2, on='datetime', suffix=f'_{sym_2}', how='full'
    ).rename({'price': f'price_{sym_1}', 'qty': f'qty_{sym_1}'}
    ).drop(f'datetime_{sym_2}').with_columns(
    pl.col(f"price_{sym_2}")
      .least_squares.rolling_ols(
          f"price_{sym_1}",
          window_size=roll_wind,
          mode="coefficients",
          add_intercept=True
    ).alias("regression_coef").shift(1)
    ).with_columns([
    pl.col("regression_coef").struct.field(f"price_{sym_1}").alias("beta"),
    pl.col("regression_coef").struct.field("const").alias("alpha")
    ]).drop('regression_coef'
    ).with_columns(
        (pl.col(f'price_{sym_2}') - (pl.col('alpha') + pl.col('beta') * pl.col(f'price_{sym_1}'))
        ).alias('spread')
    ).with_columns(
        pl.col('spread').rolling_mean(window_size=roll_wind).alias('mean'),
        pl.col('spread').rolling_std(window_size=roll_wind).alias('std')
    ).filter(pl.col(datetime) >= start_date
    )

    return stat_df.drop(f'price_{sym_1}', f'price_{sym_2}', f'qty_{sym_1}', f'qty_{sym_2}')

class Trader():
    def __init__(self, sym_1, sym_2, balance, max_order_size, fee_perc):
        self.sym_1 = sym_1
        self.sym_2 = sym_2
        self.balance = balance
        self.max_order_size = max_order_size
        self.fee_perc = fee_perc

        self._set_environment()

    def _set_environment(self):
        self.qty_1 = 0
        self.qty_2 = 0
        self.pos_side = None

    def _calculate_qty(self, price1: float, price2: float, beta: float) -> tuple:
        """
        Рассчитывает количество двух криптоактивов для открытия ордера на основании параметра beta.

        Args:
            price1: Цена первой монеты (например, RVN/USDT)
            price2: Цена второй монеты (например, T/USDT)
            usdt_amount: Общая сумма для инвестирования в USDT
            beta: Коэффициент соотношения количества монет (Q1 = beta * Q2)

        Returns:
            (quantity1, quantity2) - количество каждой из монет
        """
        beta = abs(beta)
        adjusted_amount = self.max_order_size / (1 + self.fee_perc)
        denominator = price1 * beta + price2
        quantity2 = adjusted_amount / denominator
        quantity1 = beta * quantity2

        if quantity1 > 20 or quantity2 > 20:
            quantity1 = int(quantity1)
            quantity2 = int(quantity2)
        elif quantity1 > 5 or quantity2 > 5:
            quantity1 = round(quantity1, 1)
            quantity2 = round(quantity2, 1)
        elif quantity1 >= 1 or quantity2 >= 1:
            quantity1 = round(quantity1, 2)
            quantity2 = round(quantity2, 2)
        else:
            quantity1 = round(quantity1, 4)
            quantity2 = round(quantity2, 4)

        return (quantity1, quantity2)

    def open_position(self, row: pl.Series, pos_side: str):
        # Сначала найдём размер позиции для каждой монеты в зависимости от beta
        price_1 = row[f'close_{self.sym_1}']
        price_2 = row[f'close_{self.sym_2}']
        beta = row['beta']

        self.qty_1, self.qty_2 = self._calculate_qty(price_1, price_2, beta)
        fee_1 = price_1 * self.qty_1 * self.fee_perc
        fee_2 = price_2 * self.qty_2 * self.fee_perc
        # Рассчитаем движение средств
        self.pos_side = pos_side
        if self.pos_side == 'long':
            self.balance = self.balance - price_1 * self.qty_1 + price_2 * self.qty_2 - fee_1 - fee_2
        elif self.pos_side == 'short':
            self.balance = self.balance + price_1 * self.qty_1 - price_2 * self.qty_2 - fee_1 - fee_2

    def close_position(self, row: pl.Series):
        price_1 = row[f'close_{self.sym_1}']
        price_2 = row[f'close_{self.sym_2}']
        fee_1 = price_1 * self.qty_1 * self.fee_perc
        fee_2 = price_2 * self.qty_2 * self.fee_perc

        if self.pos_side == 'long':
            self.balance = self.balance + price_1 * self.qty_1 - price_2 * self.qty_2 - fee_1 - fee_2
        elif self.pos_side == 'short':
            self.balance = self.balance - price_1 * self.qty_1 + price_2 * self.qty_2 - fee_1 - fee_2

        self._set_environment()


def run_simulation(sym_1: str,
                   sym_2: str,
                   period: str,
                   roll_wind: int,
                   start_date: datetime,
                   end_date: datetime,
                   dev_in: float,
                   dev_out: float,
                   balance: int,
                   max_order_size: int,
                   fee_perc: float,
                   verbose=False):
    stat_df = make_stat_df(sym_1, sym_2, period=period, roll_wind=roll_wind, start_date=start_date, end_date=end_date)
    data_df = make_1s_df(sym_1, sym_2, start_date, end_date)

    data_df = data_df.sort(by="datetime").join_asof(stat_df.sort(by="datetime"),
                                                    on="datetime", strategy="backward"
                                        ).fill_null(strategy='forward')
    data_df = data_df.drop('spread', f'qty_{sym_1}', f'qty_{sym_2}').with_columns(
        pl.col("beta").round(2).cast(pl.Float32)
    )

    data_df = data_df.with_columns(
            (pl.col(f'close_{sym_2}') - (pl.col('alpha') + pl.col('beta') * pl.col(f'close_{sym_1}'))).alias('curr_spread'),
        ).with_columns(
            ((pl.col('curr_spread') - pl.col('mean')) / pl.col('std')).alias('curr_dev')
        )
    data_df = data_df.with_columns(
            pl.when(pl.col("curr_dev") > dev_in).then(1).otherwise(0).alias(f"long_{sym_1}"),
            pl.when(pl.col("curr_dev") < -dev_in).then(1).otherwise(0).alias(f"short_{sym_1}"),
            pl.when(pl.col("curr_dev") < -dev_out).then(1).otherwise(0).alias(f"long_out"),
            pl.when(pl.col("curr_dev") > dev_out).then(1).otherwise(0).alias(f"short_out"),
        )

    balance_hist = []
    balance_hist.append({'date': data_df['datetime'][0], 'balance': balance})

    order_status = None
    short_price_in = None

    trader = Trader(sym_1, sym_2, balance=balance, max_order_size=max_order_size, fee_perc=fee_perc)

    for row in data_df.iter_rows(named=True):
        if order_status is None and trader.balance < 0:
            print(f'Залился {row["datetime"]}')
            break

        # --- Закрываем ордер, если есть команда на закрытие ---
        if order_status == 'long_close':
            date = row['datetime']
            price_1, price_2 = row[f'close_{sym_1}'], row[f'close_{sym_2}']
            qty_1, qty_2 = trader.qty_1, trader.qty_2

            trader.close_position(row)
            balance_hist.append({'date': date, 'balance': trader.balance})

            short_price_in = None
            order_status = None

            if verbose:
                print(f'[CLOSE ORDER] {date} sell {qty_1} {sym_1} at {price_1}, buy {qty_2} {sym_2} at {price_2}. Balance: {trader.balance}')
        elif order_status == 'short_close':
            date = row['datetime']
            price_1, price_2 = row[f'close_{sym_1}'], row[f'close_{sym_2}']
            qty_1, qty_2 = trader.qty_1, trader.qty_2

            trader.close_position(row)
            balance_hist.append({'date': date, 'balance': trader.balance})

            short_price_in = None
            order_status = None

            if verbose:
                print(f'[CLOSE ORDER] {date} sell {qty_2} {sym_2} at {price_2}, buy {qty_1} {sym_1} at {price_1}. Balance: {trader.balance}')

        # --- Открываем ордер, если есть команда на открытие ---
        if order_status == 'long_open':
            order_status = 'filled'
            short_price_in = row[f'close_{sym_2}'] # Сохраняем цену входа в шорт для проверки возможной ликвидации позиции
            trader.open_position(row, pos_side='long')
            date = row['datetime']
            price_1, price_2 = row[f'close_{sym_1}'], row[f'close_{sym_2}']
            qty_1, qty_2 = trader.qty_1, trader.qty_2

            if verbose:
                print(f'[OPEN ORDER] {date} buy {qty_1} {sym_1} at {price_1}, sell {qty_2} {sym_2} at {price_2}')
        elif order_status == 'short_open':
            order_status = 'filled'
            short_price_in = row[f'close_{sym_1}'] # Сохраняем цену входа в шорт для проверки возможной ликвидации позиции
            trader.open_position(row, pos_side='short')
            date = row['datetime']
            price_1, price_2 = row[f'close_{sym_1}'], row[f'close_{sym_2}']
            qty_1, qty_2 = trader.qty_1, trader.qty_2

            if verbose:
                print(f'[OPEN ORDER] {date} buy {qty_2} {sym_2} at {price_2}, sell {qty_1} {sym_1} at {price_1}')

        # --- Проверяем условие входа в сделку ---
        if row[f'long_{sym_1}'] and order_status is None:
            order_status = 'long_open'
        elif row[f'short_{sym_1}'] and order_status is None:
            order_status = 'short_open'

        # --- Проверяем условие выхода из сделки ---
        if row[f'long_out'] and order_status == 'filled' and trader.pos_side == 'long':
            order_status = 'long_close'
        elif row[f'short_out'] and order_status == 'filled' and trader.pos_side == 'short':
            order_status = 'short_close'

        # --- Проверяем угрозу ликвидации шортовой сделки ---
        if order_status == 'filled' and trader.pos_side == 'long':
            curr_short_price = row[f'close_{sym_2}']
            if curr_short_price > 1.8 * short_price_in:
                if verbose:
                    print(f'Закрываем ордер из-за угрозы ликвидации! date: {row['datetime']}')
                order_status = 'long_close'
        elif order_status == 'filled' and trader.pos_side == 'short':
            curr_short_price = row[f'close_{sym_1}']
            if curr_short_price > 1.8 * short_price_in:
                if verbose:
                    print(f'Закрываем ордер из-за угрозы ликвидации! date: {row['datetime']}')
                order_status = 'short_close'

    return data_df, pl.DataFrame(balance_hist)

def analyze_strategy(df, start_date, end_date):
    metrics = dict()
    metrics["start_date"] = df["date"][0]
    metrics["end_date"] = df["date"][-1]
    metrics['total_days'] = (df["date"][-1] - df["date"][0]).days
    metrics['n_deals'] = len(df) - 1
    metrics['initial_balance'] = int(df['balance'][0])
    metrics['final_balance'] = int(df['balance'][-1])
    metrics["total_return"] = round((metrics['final_balance'] / metrics['initial_balance'] - 1) * 100, 1)

    print(f'final_balance: {metrics['final_balance']}, n_deals: {metrics['n_deals']}')
    try:
        metrics["annual_return"] = round(((metrics['final_balance'] / metrics['initial_balance']
                                          ) ** (365 / metrics['total_days']) - 1) * 100, 1)
    except TypeError:
        metrics["annual_return"] = -100
    except ZeroDivisionError:
        metrics["annual_return"] = 0
        return metrics

    df = df.with_columns(
        (pl.col("balance") - pl.col("balance").shift(1)).alias("absolute_change"),
        (((pl.col("balance") / pl.col("balance").shift(1)) - 1) * 100).alias("percent_change")
    )

    # --- Показатели просадки (Drawdown) ---
    df = df.with_columns(
        pl.col("balance").cum_max().alias("cum_max")
    )
    df = df.with_columns(
        ((pl.col("balance") / pl.col("cum_max") - 1) * 100).alias("drawdown")
    )

    metrics['max_drawdown'] = round(df.select(pl.col("drawdown")).min().item(), 1)

    # --- Трейдерские показатели ---
    returns_df = df.filter(pl.col("percent_change").is_not_null())
    metrics['avg_return'] = round(returns_df.select(pl.col("percent_change")).mean().item(), 2)
    metrics['std_return'] = round(returns_df.select(pl.col("percent_change")).std().item(), 2)


    years = (end_date - start_date).days / 365.25
    metrics['trades_per_year'] = round(df.filter(pl.col("percent_change").is_not_null()).height / years, 1)
    metrics['sharpe_ratio'] = round((metrics['avg_return'] / metrics['std_return']) * np.sqrt(metrics['trades_per_year']), 2)

    # Коэффициент Сортино
    # Вычисляем стандартное отклонение отрицательных доходностей (downside deviation)
    returns_array = np.array(returns_df.select("percent_change").to_series())
    downside_std = np.std(returns_array[returns_array < 0]) if np.any(returns_array < 0) else np.nan
    metrics['sortino_ratio'] = round((metrics['avg_return'] / downside_std) * np.sqrt(metrics['trades_per_year']
                                        ) if downside_std and downside_std != 0 else np.nan, 2)

    # Коэффициент Калмара: отношение CAGR к абсолютной величине максимальной просадки
    metrics['calmar_ratio'] = round(metrics["annual_return"] / abs(metrics['max_drawdown'])
                                    if metrics['max_drawdown'] != 0 else np.nan, 2)

    # --- Аналитика по сделкам ---
    profitable_trades = returns_df.filter(pl.col('percent_change') > 0)
    losing_trades = returns_df.filter(pl.col('percent_change') < 0)

    metrics['winning_trades'] = profitable_trades.height
    metrics['losing_trades'] = losing_trades.height

    metrics['avg_profit'] = round(profitable_trades.select('percent_change').mean().item()
                                  if metrics['winning_trades'] > 0 else 0, 2)
    metrics['avg_loss'] = round(losing_trades.select('percent_change').mean().item()
                                if metrics['losing_trades'] > 0 else 0, 2)
    metrics['max_profit'] = round(profitable_trades.select('percent_change').max().item()
                                  if metrics['winning_trades'] > 0 else 0, 2)
    metrics['max_loss'] = round(losing_trades.select('percent_change').min().item()
                                if metrics['losing_trades'] > 0 else 0, 2)
    metrics['win_ratio'] = round(metrics['winning_trades'] / metrics['n_deals']
                                 if metrics['n_deals'] > 0 else 0, 2)
    metrics['avg_usdt_per_deal'] = round((metrics['final_balance'] - metrics['initial_balance'])
                                         / metrics['n_deals'], 2)
    metrics['expected_return'] = round(metrics['win_ratio'] * metrics['avg_profit'] +
                                       (1 - metrics['win_ratio']) * metrics['avg_loss'], 2)

    total_profit = profitable_trades['absolute_change'].sum() if metrics['winning_trades'] > 0 else 0
    total_loss = losing_trades['absolute_change'].sum() if metrics['losing_trades'] > 0 else 0
    metrics['profit_factor'] = round(abs(total_profit / total_loss), 2) if total_loss != 0 else float('inf')

    return metrics
