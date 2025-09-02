import heapq
import redis
from datetime import datetime
import polars as pl
import orjson

def init_exchanges(redis_client: redis.Redis, exchanges: list):
    """
    Записывает список бирж в Redis при старте программы.
    """
    # Записываем как JSON-строку с TTL=3 дня
    redis_client.set("exchanges", orjson.dumps(exchanges), ex=60*60*24*3)
    print("Список бирж инициализирован в Redis")

def get_exc_list(redis_client: redis.Redis) -> list:
    """
    Возвращает кэшированный список бирж из Redis.
    """
    data = redis_client.get("exchanges")
    if not data:
        raise ValueError("Список бирж не инициализирован! Запустите init_module.py")
    return orjson.loads(data)

def save_prices(redis_client, data: dict, top_token_list: list):
    pipe = redis_client.pipeline()
    for exchange, symbols in data.items():
        # Создаем хеш для биржи вида crypto:{exchange}
        hash_key = f"crypto:{exchange}"
        mapping = {
            symbol: orjson.dumps(fields)
            for symbol, fields in symbols.items()
            if symbol in top_token_list
        }
        if mapping:
            pipe.hset(hash_key, mapping=mapping)
    pipe.execute()

def read_prices(redis_client, exchange_keys) -> pl.DataFrame:
    pipe = redis_client.pipeline()
    for key in exchange_keys:
        pipe.hgetall(key)
    all_data = pipe.execute()

    records = []
    for exchange_key, hash_data in zip(exchange_keys, all_data):
        exchange = exchange_key.split(":")[1]
        for symbol, json_str in hash_data.items():
            try:
                record = orjson.loads(json_str)
                record["exchange"] = exchange
                record["symbol"] = symbol
                records.append(record)
            except Exception:
                continue

    return pl.DataFrame(records)

def get_exchanges(redis_client: redis.Redis) -> list:
    """
    Возвращает кэшированный список бирж из Redis.
    """
    data = redis_client.get("crypto:exchanges")
    if not data:
        raise ValueError("Список бирж не инициализирован! Запустите init_module.py")
    return orjson.loads(data)

class RedisManager():
    db_num = {'orderbooks': 0, 'prices': 1, 'orders': 2, 'trades': 3}

    def __init__(self, db_name: str,
                 group_name: str = None,
                 consumer_name: str = None,
                 read_existing: bool = True):
        assert db_name in ('orderbooks', 'prices', 'orders', 'trades'), "db_name should be in ('orderbooks', 'prices', 'orders', 'trades')"
        self.db_name = db_name
        self.redis_client = redis.Redis(db=self.db_num[db_name], decode_responses=True)

        # Configure consumer group
        self.group_name = group_name or f"{db_name}_group"
        self.consumer_name = consumer_name or 'jaref_bot'
        self.read_existing = read_existing

        # Ensure consumer group exists for each stream when first accessed
        self._groups_initialized = set()

    def _ensure_group(self, stream_key: str):
        """
        Create the consumer group on the stream if not already created.
        Use '0' to read existing messages on first consumption if read_existing=True,
        otherwise '$' to read only new messages.
        """
        if stream_key in self._groups_initialized:
            return
        start_id = '0' if self.read_existing else '$'
        try:
            self.redis_client.xgroup_create(
                name=stream_key,
                groupname=self.group_name,
                id=start_id,
                mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            # Ignore BUSYGROUP if group already exists
            if "BUSYGROUP" not in str(e):
                raise
        # After first group creation, subsequent reads should use '>'
        self._groups_initialized.add(stream_key)

    def get_keys(self):
        return self.redis_client.keys()


    def get_orderbooks(self, n_levels):
        assert self.db_name == 'orderbooks', 'Переключитесь на orderbooks таблицу!'

        sides = ['bid', 'ask']
        orderbook_data = []
        cursor = 0
        pattern = 'orderbook:*:*:*:update_time'

        while True:
            cursor, keys = self.redis_client.scan(cursor, pattern, count=10_000)
            if not keys:
                if cursor == 0:
                    break
                continue

            pipeline = self.redis_client.pipeline()
            for key in keys:
                pipeline.hget(key, 'cts')
                parts = key.split(':')
                exchange, market_type, symbol = parts[1:4]
                for side in sides:
                    orderbook_key = f'orderbook:{exchange}:{market_type}:{symbol}:{side}s'
                    pipeline.hgetall(orderbook_key)
            results = pipeline.execute()

            chunk_size = 1 + len(sides)
            for i, key in enumerate(keys):
                base_index = i * chunk_size
                ts_raw = results[base_index]
                try:
                    ts = int(ts_raw) if ts_raw is not None else None
                except (ValueError, TypeError):
                    ts = None
                update_time_readable = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None
                parts = key.split(':')
                exchange, market_type, symbol = parts[1:4]
                orderbook_data_item = {
                    'exchange': exchange,
                    'market_type': market_type,
                    'symbol': symbol,
                    'ts': ts,
                    'update_time': update_time_readable
                }

                for j, side in enumerate(sides):
                    orderbook_value = results[base_index + 1 + j] or {}
                    # Используем heapq для получения топ-n уровней
                    if orderbook_value:
                        if side == 'bid':
                            top_levels = heapq.nlargest(n_levels, orderbook_value.items(),
                                                        key=lambda kv: float(kv[0]))
                        else:
                            top_levels = heapq.nsmallest(n_levels, orderbook_value.items(),
                                                         key=lambda kv: float(kv[0]))
                        for level, (price, volume) in enumerate(top_levels):
                            orderbook_data_item[f"{side}price_{level}"] = float(price)
                            orderbook_data_item[f"{side}volume_{level}"] = float(volume)
                orderbook_data.append(orderbook_data_item)

            if cursor == 0:
                break

        try:
            res_df = pl.DataFrame(orderbook_data).with_columns(
                pl.col("update_time").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"))
        except pl.exceptions.ColumnNotFoundError:
            res_df = pl.DataFrame()

        return res_df


    def add_order(self, exchange, token, qty, side, ord_id, ts, status):
        assert self.db_name == 'orders', 'Переключитесь на orders таблицу!'
        self.redis_client.hset(name=f'pending_orders:{exchange}:{token}',
                               mapping={'qty': qty, 'side': side, 'id': ord_id,
                                        'ts': ts, 'status': status})

    def delete_order(self, exchange, token):
        assert self.db_name == 'orders', 'Переключитесь на orders таблицу!'
        self.redis_client.delete(f'pending_orders:{exchange}:{token}')

    def get_pending_orders(self):
        assert self.db_name == 'orders', 'Переключитесь на orders таблицу!'
        orders = {}

        for key in self.redis_client.scan_iter("pending_orders:*"):
            # Ключ имеет вид "pending_orders:{exchange}:{token}"
            try:
                _, exchange, token = key.split(":")
            except ValueError:
                # Если формат ключа не соответствует ожидаемому, пропускаем его
                continue

            # Получаем все поля хэша для данного ключа (данные уже в виде строк)
            order_data = self.redis_client.hgetall(key)
            orders.setdefault(exchange, {})[token] = order_data

        return orders

    def clear_orders_table(self):
        assert self.db_name == 'orders', 'Переключитесь на orders таблицу!'
        self.redis_client.flushdb()

    def add_trade_to_stream(self, exc: str, ts: int, symbol: str, side: str, price: float, volume: float):
        assert self.db_name == 'trades', 'Переключитесь на trades таблицу!'
        stream_key = f"trades:{symbol}"

        entry = {
            "exc": exc,
            "ts": str(ts),
            "side": side,
            "price": str(price),
            "volume": str(volume),
        }

        self.redis_client.xadd(stream_key, entry, maxlen=10_000, approximate=True)

    def read_trades_stream(self, symbol: str = None):
        assert self.db_name == 'trades', 'Переключитесь на trades таблицу!'

        # Determine streams
        if symbol:
            streams = [f"trades:{symbol}"]
        else:
            streams = [key for key in self.get_keys() if key.startswith('trades:')]

        # Ensure groups on each stream
        for key in streams:
            self._ensure_group(key)

        # Prepare to read only new deliveries (use '>' always)
        streams_dict = {key: '>' for key in streams}

        # After initial read, no need to deliver existing again
        self.read_existing = False

        # Read pending messages
        raw = self.redis_client.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams=streams_dict
        )

        result = {}
        for stream_key, messages in raw:
            result[stream_key] = []
            for message_id, data in messages:
                result[stream_key].append((message_id, data))
                # Acknowledge processing
                self.redis_client.xack(stream_key, self.group_name, message_id)
        return result
