import polars as pl
import pandas as pd

def get_arbitrage_fund(df, edge):
    if isinstance(df, pd.DataFrame):
        df = pl.from_pandas(df)

    return df.pivot(on='exchange', index='token',
         values=('funding_rate', 'fund_interval', 'next_fund_time')
    ).with_columns(
    (
        pl.max_horizontal(["funding_rate_okx", "funding_rate_gate", "funding_rate_bybit"]) -
        pl.min_horizontal(["funding_rate_okx", "funding_rate_gate", "funding_rate_bybit"])
    ).alias("max_diff")
    ).filter(pl.col('max_diff') > edge
    ).sort(by='max_diff', descending=True
    ).rename({'funding_rate_gate': 'fr_gate', 'funding_rate_bybit': 'fr_bybit', 'funding_rate_okx': 'fr_okx',
             'fund_interval_gate': 'int_gate', 'fund_interval_bybit': 'int_bybit', 'fund_interval_okx': 'int_okx',
             'next_fund_time_bybit': 'nft_bybit', 'next_fund_time_gate': 'nft_gate', 'next_fund_time_okx': 'nft_okx'}
    )
