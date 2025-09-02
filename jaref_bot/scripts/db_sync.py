import polars as pl
import asyncio
from datetime import datetime


from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.strategies.arbitrage import get_best_prices

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(db_params)
ob_manager = RedisManager('orderbooks')
trades_manager = RedisManager('trades')


async def sync_orderbooks():
    while True:
        try:
            tdf = ob_manager.get_orderbooks(n_levels=5)
            last_update = tdf.select('ts').max().item()
            time_delta = int(datetime.timestamp(datetime.now())) - last_update

            if time_delta > 5:
                # print('Данные давно не обновлялись.')
                await asyncio.sleep(1)
                continue

            bp = get_best_prices(tdf)
            postgre_manager.copy_data_from_redis(bp)
            await asyncio.sleep(1)
        except pl.exceptions.ColumnNotFoundError:
            await asyncio.sleep(10)
        except KeyboardInterrupt:
            break

async def main():
    while True:
        try:
            sync_orderbooks()
        except KeyboardInterrupt:
            print('Завершение работы.')
            break

if __name__ == "__main__":
    asyncio.run(main())
