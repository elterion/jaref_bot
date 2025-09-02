from time import sleep
from datetime import datetime
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.db.postgres_manager import DBManager
import polars as pl

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

redis_manager = RedisManager(db_name='orderbooks')

while True:
    try:
        df = redis_manager.get_orderbooks(1)  # глубина стакана = 1
        ct = df['update_time'].min().strftime('%Y-%m-%d %H:%M:%S')

        for row in df.iter_rows(named=True):
            if int(datetime.timestamp(datetime.now())) - row['ts'] <= 1:
                db_manager.add_orderbook(
                    exchange=row["exchange"],
                    market_type=row["market_type"],
                    symbol=row["symbol"],
                    time=row["update_time"],
                    bid_price=row["bidprice_0"],
                    bid_volume=row["bidvolume_0"],
                    ask_price=row["askprice_0"],
                    ask_volume=row["askvolume_0"]
                )

        print(f'Последнее обновление: {ct}', end='\r')
        sleep(1)
    except KeyboardInterrupt:
        print('\nЗавершение работы.')
        break
    except pl.exceptions.ColumnNotFoundError:
        ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{ct}: No Data!')
        sleep(5)
    except Exception as err:
        print('\n', err)
        break
