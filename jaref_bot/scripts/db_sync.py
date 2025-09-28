from time import sleep
from datetime import datetime
from zoneinfo import ZoneInfo

from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.db.postgres_manager import DBManager
import polars as pl

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
db_manager = DBManager(db_params)

redis_manager = RedisManager(db_name='orderbooks')
redis_sys = RedisManager(db_name = 'system_state')

buffer_frames = []   # список polars.DataFrame за последние 5 секунд
sec_counter = 0

while True:
    try:
        # Устанавливаем heartbeat отметку в Redis
        redis_sys.set_system_state('db_sync', 1)

        sec_counter += 1

        df = redis_manager.get_orderbooks(1).drop('exchange', 'market_type')
        min_time = df['update_time'].min().strftime('%Y-%m-%d %H:%M:%S')

        ct = datetime.now(ZoneInfo("Europe/Moscow")).replace(microsecond=0)
        cts = int(datetime.timestamp(ct))

        buffer_frames.append(df)

        if cts % 5 == 0 and sec_counter > 3:
            if len(buffer_frames) == 0:
                print(f'{ct:%Y-%m-%d %H:%M:%S}: No Data!')
                pass
            else:
                full = pl.concat(buffer_frames, how='vertical')
                full = full.sort(by='ts')

                agg_df = full.group_by('symbol').agg([
                    pl.col('update_time').max().alias('time'),
                    pl.col('bidprice_0').last().alias('bid_price'),
                    pl.col('bidvolume_0').mean().alias('bid_size'),
                    pl.col('askprice_0').last().alias('ask_price'),
                    pl.col('askvolume_0').mean().alias('ask_size'),
                ]).with_columns(
                    pl.lit(ct).alias('time')
                ).rename({'symbol': 'token'})

                sec_counter = 0
                buffer_frames = []

                db_manager.add_orderbook_bulk(agg_df)

        print(f'Последнее обновление: {min_time}', end='\r')
        sleep(1)
    except KeyboardInterrupt:
        print('\nЗавершение работы.')
        break
    except pl.exceptions.ColumnNotFoundError:
        print(f'{ct:%Y-%m-%d %H:%M:%S}: No Data!')
        sleep(5)
    except Exception as err:
        print('\n', err)
        break
