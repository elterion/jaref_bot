import pandas as pd
import polars as pl
from copy import deepcopy
from decimal import Decimal


def get_best_prices(orderbook_data: pl.DataFrame) -> pl.DataFrame:
    return orderbook_data.rename({"symbol": "token"}).select([
        pl.col("exchange"),
        pl.col("token"),
        pl.col("bidprice_0"),
        pl.col("askprice_0"),
        ]).group_by(["exchange", "token"]
        ).agg([
          pl.col("askprice_0").min().alias("ask"),
          pl.col("bidprice_0").max().alias("bid")
      ])

def find_tokens_to_open_order(current_data,
                              stats_data,
                              max_mean,
                              min_std,
                              std_coef,
                              min_edge,
                              min_dist):
    best_prices = get_best_prices(current_data)
    return best_prices.join(best_prices, on="token", how="full",
                            coalesce=True, suffix='_short'
        ).filter(pl.col('exchange') != pl.col('exchange_short')
        ).rename({"exchange": "long_exc", "ask": "ask_long", "bid": "bid_long",
                  "exchange_short": "short_exc"}
        ).with_columns(
           ((pl.col('bid_short') / pl.col('ask_long') - 1) * 100).alias('curr_diff')
        ).join(stats_data, on=["token", "long_exc", "short_exc"], how="inner"
        ).filter(pl.col('std') > min_std
        ).filter(pl.col('mean') < max_mean
        ).with_columns([
            (pl.col('mean') + std_coef * pl.col('std')).alias('thresh'),
            ((pl.col('curr_diff') - pl.col('mean')) / pl.col('std')).alias('dev')
            ]
        ).filter((pl.col('curr_diff') > pl.col('thresh')) & (pl.col('curr_diff') > min_edge)
        ).with_columns((pl.col('mean') + 2.5 * pl.col('std') + pl.col('cmean') + 1.5 * pl.col('cstd')).alias('dist')
        ).filter((pl.col('dev') > std_coef) & (pl.col('curr_diff') > min_edge) & (pl.col('dist') > min_dist)
        ).select('token', 'long_exc', 'short_exc', 'ask_long', 'bid_short',
                 'mean', 'std', 'thresh', 'curr_diff', 'dev', 'cmean', 'cstd', 'dist')

def find_tokens_to_close_order(current_data,
                                current_orders,
                                stats_data,
                                std_coef=None,
                                min_profit=None,
                                min_edge=None):
    best_prices = get_best_prices(current_data)

    try:
        agg = current_orders.join(best_prices, on=("token", "exchange"), how="inner"
            ).pivot(index="token", on="order_side",
                values=["ask", "bid", 'exchange', 'qty', 'usdt_amount', 'usdt_fee', 'leverage']
            ).with_columns([
                ((pl.col('bid_buy') / pl.col('ask_sell') - 1) * 100).alias('diff'),
                (pl.col('usdt_fee_buy') + pl.col('usdt_fee_sell')).alias('usdt_fee_open')
            ]).rename(
                {'exchange_buy': 'long_exc', 'exchange_sell': 'short_exc', 'qty_buy': 'qty',
                'ask_sell': 'short_price', 'bid_buy': 'long_price', 'leverage_buy': 'leverage'}
            ).join(stats_data, on=('token', 'long_exc', 'short_exc'), how="inner"
            ).with_columns([
                (pl.col('qty') * pl.col('long_price') - pl.col('usdt_amount_buy')).alias('lm_profit'),
                (pl.col('usdt_amount_sell') - pl.col('qty') * pl.col('short_price')).alias('sm_profit')
            ]).with_columns([
                ((pl.col('lm_profit') + pl.col('sm_profit') - 2 * pl.col('usdt_fee_open')) * pl.col('leverage')).alias('profit'),
                ((pl.col('diff') - pl.col('cmean')) / pl.col('cstd')).alias('deviation')
            ]).drop(['qty_sell', 'leverage_sell',
                    'usdt_fee_buy', 'usdt_fee_sell', 'usdt_fee_open', 'usdt_amount_buy',
                    'usdt_amount_sell', 'short_price', 'long_price', 'leverage',
                    'ask_buy', 'bid_sell'])
    except pl.exceptions.ColumnNotFoundError:
        return None

    return agg.filter((pl.col('deviation') > std_coef) &
                      (pl.col('cmean') + std_coef * pl.col('cstd') > min_edge))

def get_open_volume(long_orderbook, short_orderbook, min_edge, max_usdt, debug=False):
    total_volume = 0
    total_buy_amount = 0
    total_sell_amount = 0
    sell_idx = 0
    buy_idx = 0
    edge = None

    sell_bids = deepcopy(short_orderbook)
    buy_asks = deepcopy(long_orderbook)

    while sell_idx < len(sell_bids) and buy_idx < len(buy_asks):
        best_bid_price, best_bid_volume = sell_bids[sell_idx]
        best_ask_price, best_ask_volume = buy_asks[buy_idx]

        current_edge = (best_bid_price / best_ask_price - 1) * 100
        if debug:
            print(f'short vol: {best_bid_volume}, price: {best_bid_price}; long vol: {best_ask_volume}, price: {best_ask_price}; edge: {current_edge:.3f}', end=': ')

        if current_edge < min_edge:
            if debug:
                print('break')
            break  # Дальнейшие уровни не подходят
        if debug:
            print()

        edge = current_edge

        max_possible_volume = min(best_bid_volume, best_ask_volume)
        potential_cost = best_ask_price * max_possible_volume

        if debug:
            print(f'{max_possible_volume=}; {potential_cost=}')

        # Проверяем, не превысит ли это max_usdt
        if total_buy_amount + potential_cost > max_usdt:
            remaining = max_usdt - total_buy_amount
            if remaining <= 0:
                break
            volume = remaining / best_ask_price
            max_possible_volume = min(volume, max_possible_volume)
            potential_cost = best_ask_price * max_possible_volume
            if debug:
                print(f'Overflow. {remaining=}; {volume=}; {max_possible_volume=}; {potential_cost=}')

        # Обновляем общие показатели
        total_buy_amount += potential_cost
        total_sell_amount += best_bid_price * max_possible_volume
        total_volume += max_possible_volume

        # Обновляем объёмы в ордербуках
        sell_bids[sell_idx][1] -= max_possible_volume
        buy_asks[buy_idx][1] -= max_possible_volume

        if debug:
            print(f'possible volume at this level: sell {sell_bids[sell_idx][1]}; buy {buy_asks[buy_idx][1]}')

        # Переходим к следующим уровням, если текущие исчерпаны
        if sell_bids[sell_idx][1] <= 0:
            sell_idx += 1
        if buy_asks[buy_idx][1] <= 0:
            buy_idx += 1

        if debug:
            print(f'end of iteration. {total_volume=}')

        if total_buy_amount >= max_usdt:
            break

    return {'edge': edge, 'volume': total_volume, 'usdt_amount': total_buy_amount}

def get_close_volume(long_orderbook, short_orderbook, diff_edge, min_qty, max_qty, debug=False):
    total_volume = 0
    sell_idx = 0
    buy_idx = 0
    edge = None

    sell_bids = deepcopy(long_orderbook)
    buy_asks = deepcopy(short_orderbook)

    first_iter_flag = True

    while sell_idx < len(sell_bids) and buy_idx < len(buy_asks):
        best_bid_price, best_bid_volume = sell_bids[sell_idx]
        best_ask_price, best_ask_volume = buy_asks[buy_idx]

        current_edge = (best_bid_price / best_ask_price - 1) * 100
        if debug:
            print(f'bid: {best_bid_price}, vol: {best_bid_volume}; ask: {best_ask_price}, vol: {best_ask_volume}; edge: {current_edge:.3f}; {diff_edge=:.3f}; {first_iter_flag=}', end=': ')
        if current_edge < diff_edge:
            if first_iter_flag:
                return {'edge': None, 'volume': Decimal('0')}

            if debug:
                print('break')
            break  # Дальнейшие уровни не подходят
        if debug:
            print()

        edge = current_edge

        max_possible_volume = min(best_bid_volume, best_ask_volume)
        if debug:
            print(f'possible volume: {max_possible_volume}')

        # Проверяем, не превысит ли это max_usdt
        if total_volume + max_possible_volume > max_qty:
            remaining = max_qty - total_volume
            if debug:
                print(f'{total_volume=}; {remaining=}')
            if remaining <= min_qty:
                break
            max_possible_volume = min(remaining, max_possible_volume)

        # Обновляем общие показатели
        total_volume += max_possible_volume

        # Обновляем объёмы в ордербуках
        sell_bids[sell_idx][1] -= max_possible_volume
        buy_asks[buy_idx][1] -= max_possible_volume

        if debug:
            print(f'possible volume at this level: sell {sell_bids[sell_idx][1]}; buy {buy_asks[buy_idx][1]}')

        # Переходим к следующим уровням, если текущие исчерпаны
        if sell_bids[sell_idx][1] <= 0:
            sell_idx += 1
        if buy_asks[buy_idx][1] <= 0:
            buy_idx += 1

        if debug:
            print(f'end of iteration. {total_volume=}')

        if total_volume >= max_qty:
            break

        first_iter_flag = False

    return {'edge': edge, 'volume': total_volume}

# ============== Legacy =================
def find_tokens_to_open_order_from_tickers(current_data: pd.DataFrame,
                   stats_data: pd.DataFrame,
                   max_mean: float,
                   min_std: float,
                   std_coef: float) -> pd.DataFrame:
    """
    Функция выбирает среди текущих цен (current_data) те, которые удовлеторяют
    условиям открытия ордера.
    Обозначения: diff = (max_bid / min_ask - 1) * 100 - это разница между ценой
    покупки и продажи в процентах.

    Функция выбирает только те токены, у которых в датафрейме stats_data
    стандартное отклонение величины diff больше чем параметр функции min_std,
    а текущее значение diff > mean + std_coef * std

    Args:
        current_data (pd.DataFrame): таблица с текущими ценами
        stats_data (pd.DataFrame): таблица со статистическими параметрами
            по крипто-парам
        max_mean (float): максимальное значение mean в процентах, выше которого
            все крипто-пары отсекаются. Нужно для предотвращения открытия
            сделки по ошибочным токенам.
        min_std (float): крипто-пары, у которых стандартное отклонение от
        среднего значения меньше чем min_std рассматриваться не будут
        std_coef (float): коэффициент, на который умножается стандартное
            отклонение крипто-пары. Например, при std_coef == 2.5 будут
            отфильтрованы те монеты, у которых в данный момент отклонение
            от среднего значения превышает 2.5

    Returns:
        pd.DataFrame: таблица с теми токенами, которые подходят под условия.
    """
    merged_current = current_data.merge(current_data, on='token', suffixes=('_long', '_short'))
    merged_current = merged_current[merged_current['exchange_long'] != merged_current['exchange_short']]
    merged_current['curr_diff'] = (merged_current['bid_price_short'] / merged_current['ask_price_long'] - 1) * 100
    merged_current['curr_diff_out'] = (merged_current['bid_price_long'] / merged_current['ask_price_short'] - 1) * 100

    # Переименовываем столбцы для соответствия с stats_data
    merged_current.rename(columns={'exchange_long': 'long_exc', 'exchange_short': 'short_exc'}, inplace=True)

    # Объединяем с stats_data
    combined_df = merged_current.merge(stats_data, on=['token', 'long_exc', 'short_exc'], how='inner')
    combined_df = combined_df[combined_df['std'] > min_std]
    combined_df = combined_df[combined_df['mean'] < max_mean]

    # Вычисляем порог и проверяем условие
    combined_df['thresh_in'] = combined_df['mean'] + std_coef * combined_df['std']
    combined_df['condition_met'] = combined_df['curr_diff'] > combined_df['thresh_in']

    # Фильтруем результаты
    result_df = combined_df[combined_df['condition_met']].reset_index(drop=True)
    result_df['dev_in'] = (result_df['curr_diff'].astype(float) - result_df['mean']) / result_df['std']
    result_df = result_df.merge(stats_data, left_on=('token', 'long_exc', 'short_exc'),
                      right_on=('token', 'short_exc', 'long_exc'),
                      suffixes=('_in', '_out'))

    result_df.rename(columns={'long_exc_in': 'long_exc', 'short_exc_in': 'short_exc'}, inplace=True)
    result_df.drop(['market_type_long', 'timestamp_long', 'bid_price_long', 'ask_price_long',
                   'market_type_short', 'timestamp_short', 'bid_price_short', 'ask_price_short',
                   'long_exc_out', 'short_exc_out', 'max_diff_in', 'max_diff_out'], axis=1, inplace=True)
    result_df = result_df[['token', 'long_exc', 'short_exc', 'mean_in', 'std_in', 'thresh_in', 'curr_diff',
                           'mean_out', 'std_out', 'curr_diff_out', 'dev_in']]
    return result_df

def find_tokens_to_close_order_pandas(current_data,
                                         current_orders,
                                         stats_data,
                                         close_edge=None,
                                         std_coef=None,
                                         min_profit=None,
                                         profit_breakout=None):
    """
    Функция возвращает токены, которые подходят под условия закрытия ордера.
    :param close_edge: сделка закрывается при условии current_diff > mean + close_edge
    :param std_coef: сделка закрывается при условии current_diff > mean + std_coef * std
    :param close_profit: проверка профита. Если параметр указан, тогда возвращаться будут только
                         те токены, у которых profit > close_profit
    :param profit_breakout: Если у какого-либо токена profit > profit_breakout, то он закрывается вне зависимости
                         от выполнения остальных условий

    Параметры close_edge и std_coef взаимоисключающие. Если передаётся close_edge, то std_coef будет игнорироваться
    в любом случае.
    """
    cols = ['token', 'long_exc', 'short_exc', 'qty', 'close_diff', 'mean', 'std',
       'lm_profit', 'sm_profit', 'profit', 'deviation']

    try:
        orders_df = current_orders.merge(current_data, on=('token', 'exchange', 'market_type'))
        agg = orders_df.pivot(index="token", columns="order_side",
                            values=["ask_price", "bid_price", 'exchange', 'qty', 'usdt_amount', 'usdt_fee', 'leverage'])
        agg.columns = [f"{col[0]}_{col[1]}" for col in agg.columns]
        agg = agg.reset_index()

        agg["close_diff"] = (agg["bid_price_buy"] / agg["ask_price_sell"] - 1) * 100
        agg['usdt_fee_open'] = agg['usdt_fee_buy'] + agg['usdt_fee_sell']
        agg.rename(columns={'exchange_buy': 'long_exc', 'exchange_sell': 'short_exc', 'qty_buy': 'qty',
                        'ask_price_sell': 'short_price', 'bid_price_buy': 'long_price', 'leverage_buy': 'leverage'}, inplace=True)

        # Меняем местами long_exc и short_exc для того, чтобы достать из stats_data нужные пары
        long_exc = agg['short_exc']
        agg['short_exc'] = agg['long_exc']
        agg['long_exc'] = long_exc
        agg = agg.merge(stats_data, on=('token', 'long_exc', 'short_exc'))
        agg['lm_profit'] = agg['qty'] * agg['long_price'] - agg['usdt_amount_buy']
        agg['sm_profit'] = agg['usdt_amount_sell'] - agg['qty'] * agg['short_price']
        agg['profit'] = (agg['lm_profit'] + agg['sm_profit'] - 2 * agg['usdt_fee_open']) * agg['leverage']
        agg['deviation'] = (agg['close_diff'].astype(float) - agg['mean']) / agg['std']

        agg.drop(['qty_sell', 'ask_price_buy', 'bid_price_sell', 'leverage_sell',
                'usdt_fee_buy', 'usdt_fee_sell', 'usdt_fee_open', 'usdt_amount_buy',
                'usdt_amount_sell', 'short_price', 'long_price', 'leverage',
                'max_diff'], axis=1, inplace=True)

        # Снова меняем местами long_exc и short_exc для того, чтобы вернуть изначальные
        # направления торговли, чтобы не путаться, на какой бирже в каком направлении
        # я открывал сделки
        long_exc = agg['short_exc']
        agg['short_exc'] = agg['long_exc']
        agg['long_exc'] = long_exc
    except KeyError:
        return pd.DataFrame(columns=cols)

    result = pd.DataFrame(columns = agg.columns)

    if profit_breakout:
        result = pd.concat([result, agg[agg['profit'] > profit_breakout]])

    if min_profit and (close_edge or std_coef):
        if close_edge:
            result = pd.concat([result, agg[(agg['profit'] > min_profit) & (agg['close_diff'] > agg['mean'] + close_edge)]])
        elif std_coef:
            result = pd.concat([result, agg[(agg['profit'] > min_profit) & (agg['close_diff'] > agg['mean'] + std_coef * agg['std'])]])
    elif close_edge:
        result = pd.concat([result, agg[agg['close_diff'] > agg['mean'] + close_edge]])
    elif std_coef:
        result = pd.concat([result, agg[agg['close_diff'] > agg['mean'] + std_coef * agg['std']]])

    return result.drop_duplicates()

# async def fetch_all_orderbooks(tokens, limit=10):
#     tasks = [exc_manager.get_orderbook(symbol=token, limit=limit) for token in tokens]
#     results = await asyncio.gather(*tasks)
#     return dict(zip(tokens, results))
