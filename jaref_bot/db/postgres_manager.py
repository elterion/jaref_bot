import psycopg
from psycopg.rows import dict_row
from psycopg.errors import UniqueViolation
import pandas as pd
import polars as pl
from datetime import datetime, timezone, timedelta

class DBManager:
    def __init__(self, db_params):
        self.conn = psycopg.connect(**db_params)
        self.conn.autocommit = True

    def close(self):
        self.conn.close()

    def test_add(self, bucket, exchange, market_type, token, avg_bid, avg_ask):
        query = """
        INSERT INTO market_data_5s (bucket, exchange, market_type, token, avg_bid, avg_ask)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (bucket, exchange, market_type, token, avg_bid, avg_ask))

    def update_funding_data(self, records):
        """
        Обновление данных фандинга. Если запись с таким же (token, exchange)
        уже существует, она будет заменена новыми данными.

        :param records: список кортежей, где каждый кортеж имеет вид:
                        (token, exchange, ask_price, bid_price, funding_rate, fund_interval, next_fund_time)
        """
        sql = """
        INSERT INTO funding_data (token, exchange, ask_price, bid_price, funding_rate, fund_interval, next_fund_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (token, exchange) DO UPDATE SET
            ask_price = EXCLUDED.ask_price,
            bid_price = EXCLUDED.bid_price,
            funding_rate = EXCLUDED.funding_rate,
            fund_interval = EXCLUDED.fund_interval,
            next_fund_time = EXCLUDED.next_fund_time;
        """
        if isinstance(records, pl.DataFrame):
            records = records.rows()

        with self.conn.cursor() as cursor:
            cursor.executemany(sql, records)

    def add_public_trade(self, time: datetime, exchange: str, symbol: str,
                         side: str, price: float, volume: float):
        """
        Добавляет запись о публичной сделке в таблицу `public_trades`.

        Параметры:
        ----------
        time : datetime.datetime
            Точное время сделки.
        exchange : str
            Название биржи (например: 'binance', 'bybit').
        symbol : str
            Торговая пара (например: 'BTC_USDT').
        side : str
            Направление сделки: 'buy' или 'sell'.
        price : float
            Цена актива.
        volume : float
            Объем актива.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public_trades (time, exchange, symbol, side, price, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (time, exchange, symbol, side, price, volume)
            )

    def get_public_trades(self, exchange: str, token: str, return_type: str='pandas'):
        """
        Возвращает все сделки для указанной биржи и торговой пары из таблицы `public_trades`.

        Параметры:
        ----------
        exchange : str
            Название биржи (например: 'binance').
        token : str
            Торговая пара (например: 'BTC_USDT').
        return_type : str, опционально
            Формат возвращаемых данных:
            - 'pandas' → pd.DataFrame (по умолчанию)
            - 'polars' → pl.DataFrame

        Возвращает:
        -----------
        DataFrame (pandas/polars)
            Таблица со всеми столбцами из `public_trades`, отфильтрованная по `exchange` и `symbol`.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                            SELECT * FROM public_trades
                            WHERE symbol = %s AND exchange = %s
                            ORDER BY time
                            """, (token, exchange))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                if return_type == 'pandas':
                    df = pd.DataFrame(rows, columns=columns)
                    df.reset_index(drop=True, inplace=True)
                elif return_type == 'polars':
                    df = pl.DataFrame(rows, schema=columns, orient="row")
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table: {e}")

    def get_last_trade(self, exchange, token, return_type='pandas'):
        """
        Возвращает последнюю сделку для указанной биржи и торговой пары из таблицы public_trades.

        Параметры:
        ----------
        exchange : str
            Название биржи (например: 'binance').
        token : str
            Торговая пара (например: 'BTC_USDT').
        return_type : str, опционально
            Формат возвращаемых данных:
            - 'pandas' → pd.Series (одна строка в виде Series)
            - 'polars' → pl.DataFrame (одна строка)

        Возвращает:
        -----------
        Series (pandas) или DataFrame (polars)
            Данные последней сделки.
        """
        try:
            with self.conn.cursor() as cur:
                query = """
                    SELECT *
                    FROM public_trades
                    WHERE symbol = %s AND exchange = %s
                    ORDER BY time DESC
                    LIMIT 1
                """
                cur.execute(query, (token, exchange))
                row = cur.fetchone()
                columns = [desc[0] for desc in cur.description] if cur.description else []

                if return_type == 'pandas':
                    df = pd.Series(row, index=columns)
                    # df.reset_index(drop=True, inplace=True)
                    return df
                elif return_type == 'polars':
                    return pl.DataFrame([row], schema=columns, orient="row")
                else:
                    raise ValueError(f"Unsupported return type: {return_type}")

        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch last trade: {e}")

    def get_candlestick(self, exchange: str, token: str, return_type: str='pandas'):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                            SELECT * FROM ohlcv_5min
                            WHERE symbol = %s AND exchange = %s
                            ORDER BY bucket
                            """, (token, exchange))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]

                if return_type == 'pandas':
                    df = pd.DataFrame(rows, columns=columns)
                    df.reset_index(drop=True, inplace=True)
                elif return_type == 'polars':
                    df = pl.DataFrame(rows, schema=columns, orient="row")
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table: {e}")

    def place_order(self, token, exchange, market_type, order_type, order_side,
                  qty, price, usdt_amount, realized_pnl, leverage, created_at=None):
        """Добавляет новый ордер в таблицу current_orders"""
        query = """
        INSERT INTO current_orders (token, exchange, market_type, order_type,
        order_side, qty, price, usdt_amount, realized_pnl, leverage, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (token, exchange, market_type)
        DO UPDATE SET
            qty = EXCLUDED.qty,
            price = EXCLUDED.price,
            usdt_amount = EXCLUDED.usdt_amount,
            realized_pnl = EXCLUDED.realized_pnl;
        """

        # Если created_at не передан, используем текущее время
        if created_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            created_at = datetime.now(Moscow_TZ).strftime('%Y-%m-%d %H:%M:%S')

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (token, exchange, market_type, order_type,
                    order_side, qty, price, usdt_amount, realized_pnl, leverage, created_at))
        except psycopg.IntegrityError as e:
            self.conn.rollback()
            raise UniqueViolation(f"Order '{token}' on {exchange}.{market_type} already exists.")

    def add_pair_order(self, token_1, token_2, side, qty_1, qty_2):
        """Добавляет новый ордер в таблицу pairs"""
        query = """
        INSERT INTO pairs (token_1, token_2, side, qty_1, qty_2)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (token_1, token_2)
        DO NOTHING
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (token_1, token_2, side, qty_1, qty_2))

    def delete_pair_order(self, token_1, token_2):
        """Удаляет запись из таблицы pairs по ключу (token_1, token_2)"""
        query = """DELETE FROM pairs
        WHERE token_1 = %s AND token_2 = %s"""
        with self.conn.cursor() as cur:
            cur.execute(query, (token_1, token_2))


    def close_order(self, token, exchange, market_type, qty, close_price, close_usdt_amount, close_fee, closed_at=None):
        """
        Переносит информацию из таблицы current_orders в таблицу trading_history

        Args:
            token (str): Токен
            exchange (str): Биржа
            market_type (str): Тип рынка
            qty (float): Количество
            close_price (float): Цена закрытия
            close_usdt_amount (float): Сумма в USDT при закрытии
            close_fee (float): Комиссия за закрытие
            closed_at (datetime, optional): Время закрытия ордера

        Returns:
            bool: True при успешном выполнении

        Raises:
            Exception: Если запись не найдена или произошла ошибка при выполнении
        """
        try:
            # Устанавливаем московское время для закрытия, если не указано
            if closed_at is None:
                Moscow_TZ = timezone(timedelta(hours=3))
                closed_at = datetime.now(Moscow_TZ)

            cursor = self.conn.cursor()

            # Находим запись в текущих ордерах
            cursor.execute("""
                SELECT token, exchange, market_type, order_type, order_side,
                    price as open_price, usdt_amount as open_usdt_amount,
                    qty, usdt_fee as open_fee, leverage, created_at
                FROM current_orders
                WHERE token = %s AND exchange = %s AND market_type = %s
            """, (token, exchange, market_type))

            order_record = cursor.fetchone()

            if not order_record:
                raise Exception(f"Ордер не найден: {token}, {exchange}, {market_type}")

            # Создаем словарь для данных из текущей записи
            order_data = {
                'token': order_record[0],
                'exchange': order_record[1],
                'market_type': order_record[2],
                'order_type': order_record[3],
                'order_side': order_record[4],
                'open_price': order_record[5],
                'open_usdt_amount': order_record[6],
                'qty_current': order_record[7],
                'realized_pnl': order_record[8],
                'leverage': order_record[9],
                'created_at': order_record[10]
            }

            # Начинаем транзакцию
            self.conn.autocommit = False

            try:
                # Проверяем, закрывается ли ордер полностью или частично
                if float(order_data['qty_current']) - float(qty) < 0.000001:
                    # Полное закрытие ордера

                    # Добавляем запись в историю торговли
                    cursor.execute("""
                        INSERT INTO trading_history (
                            token, exchange, market_type, order_type, order_side,
                            open_price, close_price, open_usdt_amount, close_usdt_amount,
                            qty, open_fee, close_fee, leverage, created_at, closed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        order_data['token'], order_data['exchange'], order_data['market_type'],
                        order_data['order_type'], order_data['order_side'],
                        order_data['open_price'], close_price, order_data['open_usdt_amount'],
                        close_usdt_amount, qty, order_data['open_fee'], close_fee,
                        order_data['leverage'], order_data['created_at'], closed_at
                    ))

                    # Удаляем запись из текущих ордеров
                    cursor.execute("""
                        DELETE FROM current_orders
                        WHERE token = %s AND exchange = %s AND market_type = %s
                    """, (token, exchange, market_type))

                else:
                    # Частичное закрытие ордера

                    # Вычисляем пропорцию
                    ratio = float(qty) / float(order_data['qty_current'])

                    # Пересчитываем значения
                    adjusted_open_usdt_amount = float(order_data['open_usdt_amount']) * ratio
                    adjusted_open_fee = float(order_data['open_fee']) * ratio

                    # Добавляем запись в историю торговли
                    cursor.execute("""
                        INSERT INTO trading_history (
                            token, exchange, market_type, order_type, order_side,
                            open_price, close_price, open_usdt_amount, close_usdt_amount,
                            qty, open_fee, close_fee, leverage, created_at, closed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        order_data['token'], order_data['exchange'], order_data['market_type'],
                        order_data['order_type'], order_data['order_side'],
                        order_data['open_price'], close_price, adjusted_open_usdt_amount,
                        close_usdt_amount, qty, adjusted_open_fee, close_fee,
                        order_data['leverage'], order_data['created_at'], closed_at
                    ))

                    # Обновляем количество в текущих ордерах
                    new_qty = float(order_data['qty_current']) - float(qty)
                    new_usdt_amount = float(order_data['open_usdt_amount']) - adjusted_open_usdt_amount
                    new_open_fee = float(order_data['open_fee']) - adjusted_open_fee
                    cursor.execute("""
                        UPDATE current_orders
                        SET qty = %s, usdt_amount = %s, usdt_fee = %s
                        WHERE token = %s AND exchange = %s AND market_type = %s
                    """, (new_qty, new_usdt_amount, new_open_fee, token, exchange, market_type))

                # Подтверждаем транзакцию
                self.conn.commit()
                return True

            except Exception as e:
                # Откатываем транзакцию в случае ошибки
                self.conn.rollback()
                raise Exception(f"Ошибка при закрытии ордера: {str(e)}")

            finally:
                # Возвращаем автокоммит в исходное состояние
                self.conn.autocommit = True
                cursor.close()

        except Exception as e:
            raise Exception(f"Ошибка при закрытии ордера: {str(e)}")

    def order_exists(self, table_name, token, exchange, market_type):
        """Проверяет, существует ли в таблице current_orders запись с заданным token"""
        query = f"""
        SELECT 1 FROM {table_name}
        WHERE token = %s AND exchange = %s AND market_type = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (token, exchange, market_type))
            return cur.fetchone() is not None

    def get_order(self, token):
        """Возвращает запись из таблицы current_orders по token"""
        query = "SELECT * FROM current_orders WHERE token = %s"
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (token,))
            order = cur.fetchone()
            if not order:
                raise ValueError(f"No order found with token '{token}'.")
            return dict(order)

    def delete_order(self, token, exchange, market_type):
        """Удаляет запись из таблицы current_orders по ключу (token, exchange, market_type)"""
        query = """DELETE FROM current_orders
        WHERE token = %s AND exchange = %s AND market_type = %s"""
        with self.conn.cursor() as cur:
            cur.execute(query, (token, exchange, market_type))

    def add_orderbook(self, exchange: str, market_type: str, symbol: str,
                     time: datetime, bid_price: float, bid_volume: float,
                     ask_price: float, ask_volume: float):
        """
        Сохраняет состояние биржевого стакана в таблицу raw_orderbook_data

        Args:
            exchange: название биржи
            market_type: тип рынка
            symbol: символ торговой пары
            time: время обновления
            bid_price: цена лучшего бида
            bid_volume: объем лучшего бида
            ask_price: цена лучшего аска
            ask_volume: объем лучшего аска
        """

        query = """
            INSERT INTO raw_orderbook_data (
                exchange, market_type, token, time,
                bid_price, bid_size, ask_price, ask_size
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, market_type, token, time)
            DO NOTHING;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (
                exchange, market_type, symbol, time,
                bid_price, bid_volume, ask_price, ask_volume
            ))

    def get_orderbooks(
        self,
        exchange: str,
        market_type: str,
        symbol: str | None = None,
        interval: str = "1min",   # параметр агрегации
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pl.DataFrame:
        """
        Получает историю изменения ордербука по конкретной монете

        Args:
            exchange: название биржи
            market_type: тип рынка
            symbol: символ торговой пары
            interval: окно агрегации (например: "1min", "15min", "1h")
            start_date: начало периода (UTC)
            end_date: конец периода (UTC)

        Returns:
            polars.DataFrame с историей ордербука
        """

        valid_intervals = {"1min", "1h", "4h"}
        if interval not in valid_intervals:
            raise ValueError(f"Недопустимый интервал: {interval}. Разрешено: {valid_intervals}")

        table = f"orderbook_{interval}"

        # Базовый запрос
        query = f"""
            SELECT bucket, token, exchange, market_type, bid_price, bid_size, ask_price, ask_size
            FROM {table}
            WHERE exchange = %s
            AND market_type = %s
        """

        params = [exchange, market_type]

        if symbol is not None:
            query += " AND token = %s"
            params.append(symbol)

        # Фильтры по времени
        if start_date is not None:
            query += " AND bucket >= %s"
            params.append(start_date)

        if end_date is not None:
            query += " AND bucket <= %s"
            params.append(end_date)

        query += " ORDER BY bucket;"

        with self.conn.cursor() as cur:
            cur.execute(query, tuple(params))
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()

        return pl.DataFrame(data, schema=columns, orient="row")

    def get_raw_orderbooks(self, exchange, market_type, token, start_time=None, end_time=None):
        query = """
            SELECT exchange, market_type, token, time,
                   bid_price, bid_size, ask_price, ask_size
            FROM raw_orderbook_data
            WHERE exchange = %s
              AND market_type = %s
              AND token = %s
        """

        params = [exchange, market_type, token]

        if start_time is not None:
            query += " AND time >= %s"
            params.append(start_time)

        if end_time is not None:
            query += " AND time <= %s"
            params.append(end_time)

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return pl.DataFrame(rows, schema=colnames, orient="row")


    def update_data(self, data, tokens_to_insert):
        with self.conn.cursor() as cur:
            query = """
            INSERT INTO current_data (
                exchange, market_type, token, timestamp, bid_price, ask_price
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, market_type, token)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                bid_price = EXCLUDED.bid_price,
                ask_price = EXCLUDED.ask_price;
            """
            for exchange, markets in data.items():
                exc = exchange.split('_')[0]
                market_type = "spot" if "spot" in exchange else "linear"
                for symbol, metrics in markets.items():
                    if symbol in tokens_to_insert:
                        cur.execute(query, (
                            exc,
                            market_type,
                            symbol,
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            metrics.get('bid_price'),
                            metrics.get('ask_price'),
                        ))

    def update_stats(self, df):
        """Обновляет статистику в таблице stats_data по ключу (token, long_exc, short_exc)"""
        query = """
            INSERT INTO stats_data (token, long_exc, short_exc, mean, std, cmean, cstd)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token, long_exc, short_exc)
            DO UPDATE SET
                mean = EXCLUDED.mean,
                std = EXCLUDED.std,
                cmean = EXCLUDED.cmean,
                cstd = EXCLUDED.cstd
        """

        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        with self.conn.cursor() as cursor:
            # Преобразуем DataFrame в список кортежей
            data = [
                (row.token, row.long_exc, row.short_exc, row.mean, row.std, row.cmean, row.cstd)
                for row in df.itertuples()
            ]
            try:
                cursor.executemany(query, data)
            except Exception as e:
                print(f"Error updating stats: {e}")
                self.conn.rollback()
                raise

    def copy_data_from_redis(self, df):
        """Записывает данные в таблицу market_data по ключу
        (exchange, market_type, token, timestamp)"""
        query = """
            INSERT INTO market_data (exchange, market_type, token, timestamp, ask_price, bid_price)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, market_type, token, timestamp)
            DO UPDATE SET
                exchange = EXCLUDED.exchange,
                market_type = EXCLUDED.market_type,
                token = EXCLUDED.token,
                timestamp = EXCLUDED.timestamp
        """

        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        Moscow_TZ = timezone(timedelta(hours=3))
        created_at = datetime.now(Moscow_TZ).strftime('%Y-%m-%d %H:%M:%S')

        with self.conn.cursor() as cursor:
            # Преобразуем DataFrame в список кортежей
            data = [
                (row.exchange, 'linear', row.token, created_at, row.ask, row.bid)
                for row in df.itertuples()
            ]
            try:
                cursor.executemany(query, data)
            except Exception as e:
                print(f"Error updating stats: {e}")
                self.conn.rollback()
                raise

    def clear_old_data(self, table, column, expiration_time, units):
        """
        Удаляет из таблицы 'table' все данные, которые старше 'expiration_time',
        измеренных в 'units' по столбцу 'column'.
        :param table: название таблицы, данные из которой необходимо удалить
        :param column: название столбца, содержащего временные значения
        :param expiration_time: время в часах
        :param units: единицы измерения времени: ('hour', 'hours', 'seconds', 'minutes')
        """
        assert units in ('hour', 'hours', 'seconds', 'minutes'), "units should be in ('hour', 'hours', 'seconds', 'minutes')"
        assert column in ('bucket', 'timestamp', 'time'), "column should be in ('bucket', 'time', 'timestamp')"

        query = f"DELETE FROM {table} WHERE {column} < NOW() - INTERVAL '{expiration_time} {units}';"
        with self.conn.cursor() as cur:
            cur.execute(query)

    def get_table(self, table_name, df_type='pandas'):
        """
        Получает все данные из заданной таблицы и возвращает их как pandas или polars DataFrame.
        """
        query = f"SELECT * FROM {table_name};"
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                # Получение данных и названий столбцов
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                # Преобразование в DataFrame
                if df_type == 'pandas':
                    df = pd.DataFrame(rows, columns=columns)
                    if 'id' in df.columns:
                        df = df.set_index('id')
                elif df_type == 'polars':
                    df = pl.DataFrame(rows, schema=columns, orient="row")
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table '{table_name}': {e}")

    def get_unique_tokens(self):
        """
        Возвращает список уникальных значений токенов из таблицы market_data_5s
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT token FROM market_data_5s")
            tokens = [row[0] for row in cur.fetchall()]
            return tokens

    def get_token_history(self, token):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM market_data_5s WHERE token = %s", (token,))
                # Получение данных и названий столбцов
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                # Преобразование в DataFrame
                df = pd.DataFrame(rows, columns=columns)
                return df.reset_index(drop=True)
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table: {e}")

    def get_auto_copy_trigger_state(self):
        """
        Функция проверяет, в какое значение установлено автокопирование данных
        из таблицы current_data в таблицу market_data ('ENABLE' or 'DISABLE')
        """
        query = """
            SELECT tgenabled
            FROM pg_trigger
            JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid
            WHERE pg_class.relname = 'current_data'
                AND pg_trigger.tgname = 'after_current_data_change';
        """
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            result = cur.fetchone()

        if result:
            state = result['tgenabled']
            if state == 'O':
                return "ENABLED"
            elif state == 'D':
                return "DISABLED"
        return "UNKNOWN"

    def set_auto_copy_trigger_state(self, state=''):
        """
        Включает или отключает автокопирование данных из таблицы current_data
        в долговременную память в таблицу market_data.

        :param state: 'ENABLE' or 'DISABLE'
        """
        assert state.upper() in ('ENABLE', 'DISABLE'), 'state should be ENABLE or DISABLE'
        action = "ENABLE" if state.upper() == 'ENABLE' else "DISABLE"
        with self.conn.cursor(row_factory=dict_row) as cur:
            query = f"ALTER TABLE current_data {action} TRIGGER after_current_data_change;"
            cur.execute(query)

    def get_columns(self, table_name):
        """Получить список столбцов таблицы"""
        query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            return columns

    def get_table_info(self, table_name: str, schema: str = "public") -> dict:
        """Возвращает метаданные о таблице: колонки, типы, ограничения, индексы."""
        with self.conn.cursor() as cur:
            # 1. Колонки и типы
            cur.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table_name))
            columns = cur.fetchall()

            # 2. Первичный ключ
            cur.execute("""
                SELECT
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = %s
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'PRIMARY KEY';
            """, (schema, table_name))
            pk = [row[0] for row in cur.fetchall()]

            # 3. Внешние ключи
            cur.execute("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema = %s
                  AND tc.table_name = %s
                  AND tc.constraint_type = 'FOREIGN KEY';
            """, (schema, table_name))
            fks = cur.fetchall()

            # 4. Индексы
            cur.execute("""
                SELECT
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s;
            """, (schema, table_name))
            indexes = cur.fetchall()

        # Формируем результат
        return {
            "table": table_name,
            "schema": schema,
            "columns": [
                {
                    "name": c[0],
                    "type": c[1],
                    "nullable": c[2],
                    "default": c[3]
                }
                for c in columns
            ],
            "primary_key": pk,
            "foreign_keys": [
                {"column": fk[0], "ref_table": fk[1], "ref_column": fk[2]}
                for fk in fks
            ],
            "indexes": [
                {"name": idx[0], "definition": idx[1]}
                for idx in indexes
            ]
        }

    def clear_table(self, table_name):
        """Полностью очищает указанную таблицу"""
        query = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to truncate table '{table_name}': {e}")
