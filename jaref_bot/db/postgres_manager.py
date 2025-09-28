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

    def place_order(self, token, order_type, order_side,
                  qty, price, usdt_amount, realized_pnl, leverage, created_at=None):
        """Добавляет новый ордер в таблицу current_orders"""
        query = """
        INSERT INTO current_orders (token, order_type,
        order_side, qty, price, usdt_amount, realized_pnl, leverage, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Если created_at не передан, используем текущее время
        if created_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            created_at = datetime.now(Moscow_TZ).strftime('%Y-%m-%d %H:%M:%S')

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (token, order_type,
                    order_side, qty, price, usdt_amount, realized_pnl, leverage, created_at))
        except psycopg.IntegrityError as e:
            self.conn.rollback()
            raise UniqueViolation(f"Order '{token}' already exists.")

    def add_pair_order(self, token_1, token_2, side, qty_1, qty_2):
        """Добавляет новый ордер в таблицу pairs"""
        query = """
        INSERT INTO pairs (token_1, token_2, side, qty_1, qty_2)
        VALUES (%s, %s, %s, %s, %s)
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (token_1, token_2, side, qty_1, qty_2))

    def commit_pair_order(self, token_1, token_2, side):
        """Обновляет статус ордера на 'active' по ключу (token_1, token_2, side)"""
        query = """
            UPDATE pairs
            SET status = 'active'
            WHERE token_1 = %s AND token_2 = %s AND side = %s
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (token_1, token_2, side))

    def close_pair_order(self, token_1, token_2, side):
        """Обновляет статус ордера на 'closing' по ключу (token_1, token_2, side)"""
        query = """
            UPDATE pairs
            SET status = 'closing'
            WHERE token_1 = %s AND token_2 = %s AND side = %s
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query, (token_1, token_2, side))

    def delete_pair_order(self, token_1, token_2):
        """Удаляет запись из таблицы pairs по ключу (token_1, token_2)"""
        query = """DELETE FROM pairs
        WHERE token_1 = %s AND token_2 = %s"""
        with self.conn.cursor() as cur:
            cur.execute(query, (token_1, token_2))

    def add_data_to_zscore_history(self, data):
        """
        Добавляет список записей в таблицу
        data: список кортежей в формате (ts, exchange, token_1, token_2, z_score, profit)
        """
        with self.conn.cursor() as cur:
            # Преобразуем данные в нужный формат
            records = [
                (ts, exchange, token_1, token_2, profit, z_score)
                for (ts, exchange, token_1, token_2, profit, z_score) in data
            ]

            # Выполняем массовую вставку
            cur.executemany(
                "INSERT INTO zscore_history (ts, exchange, token_1, token_2, profit, z_score) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                records
            )

    def get_zscore_history(self, token_1, token_2, start_ts, end_ts):
        query = """
            SELECT ts, time, exchange, token_1, token_2, profit, z_score
            FROM zscore_history
            WHERE token_1 = %s
              AND token_2 = %s
              AND ts >= %s
              AND ts <= %s
            ORDER BY time;
        """

        params = [token_1, token_2, start_ts, end_ts]

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return pl.DataFrame(rows, schema=colnames, orient="row")

    def close_order(self, token, qty, close_price, close_usdt_amount, close_fee, closed_at=None):
        """
        Переносит информацию из таблицы current_orders в таблицу trading_history

        Args:
            token (str): Токен
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
        # Устанавливаем московское время для закрытия, если не указано
        if closed_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            closed_at = datetime.now(Moscow_TZ).strftime('%Y-%m-%d %H:%M:%S')

        cursor = self.conn.cursor()

        # Находим запись в текущих ордерах
        cursor.execute("""
            SELECT token, order_type, order_side,
                price as open_price, usdt_amount as open_usdt_amount,
                qty, realized_pnl as realized_pnl, leverage, created_at
            FROM current_orders
            WHERE token = %s
        """, (token, ))

        order_record = cursor.fetchone()

        if not order_record:
            raise Exception(f"Ордер не найден: {token}")

        # Создаем словарь для данных из текущей записи
        order_data = {
            'token': order_record[0],
            'order_type': order_record[1],
            'order_side': order_record[2],
            'open_price': order_record[3],
            'open_usdt_amount': order_record[4],
            'qty_current': order_record[5],
            'realized_pnl': order_record[6],
            'leverage': order_record[7],
            'created_at': order_record[8]
        }

        # Начинаем транзакцию
        self.conn.autocommit = False
        real_pnl = order_data['realized_pnl'] + close_fee

        try:
            # Проверяем, закрывается ли ордер полностью или частично
            if float(order_data['qty_current']) - float(qty) < 0.000001:
                # Полное закрытие ордера

                # Добавляем запись в историю торговли
                cursor.execute("""
                    INSERT INTO trading_history (
                        token, order_type, order_side,
                        open_price, close_price, open_usdt_amount, close_usdt_amount,
                        qty, realized_pnl, leverage, created_at, closed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_data['token'], order_data['order_type'], order_data['order_side'],
                    order_data['open_price'], close_price, order_data['open_usdt_amount'],
                    close_usdt_amount, qty, real_pnl,
                    order_data['leverage'], order_data['created_at'], closed_at
                ))

                # Удаляем запись из текущих ордеров
                cursor.execute("""
                    DELETE FROM current_orders
                    WHERE token = %s
                """, (token, ))

            else:
                # Частичное закрытие ордера

                # Вычисляем пропорцию
                ratio = float(qty) / float(order_data['qty_current'])

                # Пересчитываем значения
                adjusted_open_usdt_amount = float(order_data['open_usdt_amount']) * ratio
                adjusted_pnl = float(order_data['realized_pnl']) * ratio + close_fee

                # Добавляем запись в историю торговли
                cursor.execute("""
                    INSERT INTO trading_history (
                        token, order_type, order_side,
                        open_price, close_price, open_usdt_amount, close_usdt_amount,
                        qty, realized_pnl, leverage, created_at, closed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_data['token'], order_data['order_type'], order_data['order_side'],
                    order_data['open_price'], close_price, adjusted_open_usdt_amount,
                    close_usdt_amount, qty, adjusted_pnl,
                    order_data['leverage'], order_data['created_at'], closed_at
                ))

                # Обновляем количество в текущих ордерах
                new_qty = float(order_data['qty_current']) - float(qty)
                new_usdt_amount = float(order_data['open_usdt_amount']) - adjusted_open_usdt_amount
                new_open_fee = float(order_data['open_fee']) - adjusted_pnl
                cursor.execute("""
                    UPDATE current_orders
                    SET qty = %s, usdt_amount = %s, usdt_fee = %s
                    WHERE token = %s
                """, (new_qty, new_usdt_amount, new_open_fee, token))

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


    def order_exists(self, table_name, token):
        """Проверяет, существует ли в таблице current_orders запись с заданным token"""
        query = f"""
        SELECT 1 FROM {table_name}
        WHERE token = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (token, ))
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

    def delete_order(self, token):
        """Удаляет запись из таблицы current_orders по ключу (token, )"""
        query = """DELETE FROM current_orders
        WHERE token = %s"""
        with self.conn.cursor() as cur:
            cur.execute(query, (token, ))

    def add_orderbook(self, symbol: str,
                     time: datetime, bid_price: float, bid_volume: float,
                     ask_price: float, ask_volume: float):
        """
        Сохраняет состояние биржевого стакана в таблицу tick_ob

        Args:
            symbol: символ торговой пары
            time: время обновления
            bid_price: цена лучшего бида
            bid_volume: объем лучшего бида
            ask_price: цена лучшего аска
            ask_volume: объем лучшего аска
        """

        query = """
            INSERT INTO tick_ob (
                token, time,
                bid_price, bid_size, ask_price, ask_size
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (
                symbol, time, bid_price, bid_volume, ask_price, ask_volume
            ))

    def add_orderbook_bulk(self, df):
        query = """
            INSERT INTO tick_ob (
                token, time, bid_price, bid_size, ask_price, ask_size
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (token, time)
                DO NOTHING
        """

        data = df.rows()
        with self.conn.cursor() as cur:
            cur.executemany(query, data)


    def get_orderbooks(
        self,
        symbol: str | None = None,
        interval: str = "5m",   # параметр агрегации
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pl.DataFrame:
        """
        Получает историю изменения ордербука по конкретной монете

        Args:
            symbol: символ торговой пары
            interval: окно агрегации (например: "5m", "1h")
            start_date: начало периода (UTC)
            end_date: конец периода (UTC)

        Returns:
            polars.DataFrame с историей ордербука
        """

        valid_intervals = {"5m", "1h", "4h"}
        if interval not in valid_intervals:
            raise ValueError(f"Недопустимый интервал: {interval}. Разрешено: {valid_intervals}")

        table = f"orderbook_{interval}"

        # Базовый запрос
        query = f"""
            SELECT time, token, price
            FROM {table}
        """

        params = []

        if symbol or start_date or end_date:
            query += " WHERE"

        if symbol is not None:
            query += " token = %s"
            params.append(symbol)

        # Фильтры по времени
        if start_date is not None:
            if symbol:
                query += " AND time >= %s"
            else:
                query += " time >= %s"
            params.append(start_date)

        if end_date is not None:
            if symbol or start_date:
                query += " AND time <= %s"
            else:
                query += " time <= %s"
            params.append(end_date)

        query += " ORDER BY time;"

        with self.conn.cursor() as cur:
            cur.execute(query, tuple(params))
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()

        return pl.DataFrame(data, schema=columns, orient="row")

    def get_oldest_date_in_orderbook(self, token):
        query = """
            SELECT MIN("time") AS oldest_time
            FROM tick_ob
            WHERE token = %s;
        """

        with self.conn.cursor() as cur:
            # return pl.read_database(query, cur)
            cur.execute(query, [token])
            res = cur.fetchone()

        return res[0]

    def get_tick_ob(self, token=None, start_time=None, end_time=None):
        query = """
            SELECT token, time,
                   bid_price, bid_size, ask_price, ask_size
            FROM tick_ob
        """

        if token or start_time or end_time:
            query += " WHERE"

        params = []

        if token is not None:
            query += " token = %s"
            params.append(token)

        if start_time is not None:
            if token:
                query += " AND time >= %s"
            else:
                query += " time >= %s"
            params.append(start_time)

        if end_time is not None:
            if token or start_time:
                query += " AND time <= %s"
            else:
                query += " time <= %s"
            params.append(end_time)

        query += " ORDER BY time;"

        with self.conn.cursor() as cur:
            # return pl.read_database(query, cur)
            cur.execute(query, params)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        return pl.DataFrame(rows, schema=colnames, orient="row")

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
        assert column in ('time', 'timestamp', 'time'), "column should be in ('time', 'time', 'timestamp')"

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

    def get_columns(self, table_name):
        """Получить список столбцов таблицы"""
        query = """
            SELECT column_name, data_type
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
