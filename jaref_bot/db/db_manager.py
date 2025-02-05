import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.errors import UniqueViolation
import pandas as pd
from datetime import datetime, timezone, timedelta

class DBManager:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        self.conn.autocommit = True

    def add_order(self, token, exchange, market_type, order_type, order_id, order_side,
                  price, avg_price, usdt_amount, qty, fee, created_at=None):
        """Добавляет новый ордер в таблицу current_orders"""
        query = """
        INSERT INTO current_orders (token, exchange, market_type, order_type, order_id,
        order_side, price, avg_price, usdt_amount, qty, fee, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # Если created_at не передан, используем текущее время
        if created_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            created_at = datetime.now(Moscow_TZ).strftime('%Y-%m-%d %H:%M:%S')

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (token, exchange, market_type, order_type, order_id,
                                       order_side, price, avg_price, usdt_amount, qty,
                                       fee, created_at))
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            raise UniqueViolation(f"Order with token '{token}' on {exchange} already exists.")

    def order_exists(self, token):
        """Проверяет, существует ли в таблице current_orders запись с заданным token"""
        query = "SELECT 1 FROM current_orders WHERE token = %s"
        with self.conn.cursor() as cur:
            cur.execute(query, (token,))
            return cur.fetchone() is not None

    def get_order(self, token):
        """Возвращает запись из таблицы current_orders по token"""
        query = "SELECT * FROM current_orders WHERE token = %s"
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, (token,))
            order = cur.fetchone()
            if not order:
                raise ValueError(f"No order found with token '{token}'.")
            return dict(order)

    def delete_order(self, token):
        """Удаляет запись из таблицы current_orders по token"""
        query = "DELETE FROM current_orders WHERE token = %s"
        with self.conn.cursor() as cur:
            cur.execute(query, (token,))
            # self.conn.commit()

    def close_order(self, token, exchange, market_type, close_price, close_avg_price,
                    close_usdt_amount, qty, close_fee, closed_at=None):
        """Закрывает ордер и переносит его в таблицу trading_history"""

        if closed_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            closed_at = datetime.now(Moscow_TZ)

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            # Проверка существования записи
            cur.execute("""
                SELECT order_type, order_side, price, avg_price, usdt_amount, qty, fee, created_at
                FROM current_orders
                WHERE token = %s AND exchange = %s AND market_type = %s
            """, (token, exchange, market_type))

            order = cur.fetchone()
            if not order:
                raise ValueError(f"No order found with token '{token}', exchange '{exchange}', market_type '{market_type}'.")

            # Перенос данных в таблицу trading_history
            cur.execute("""
                INSERT INTO trading_history (token, exchange, market_type, order_type, order_side,
                        open_price, close_price, open_avg_price, close_avg_price,
                        open_usdt_amount, close_usdt_amount, qty, open_fee, close_fee,
                        created_at, closed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                token, exchange, market_type, order['order_type'], order['order_side'],
                order['price'], close_price, order['avg_price'], close_avg_price, order['usdt_amount'],
                close_usdt_amount, qty, order['fee'], close_fee, order['created_at'], closed_at
            ))

            # Удаление записи из current_orders
            cur.execute("""
                DELETE FROM current_orders
                WHERE token = %s AND exchange = %s AND market_type = %s
            """, (token, exchange, market_type))

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

    def get_table(self, table_name):
        """
        Получает все данные из заданной таблицы и возвращает их как pandas DataFrame.
        """
        query = f"SELECT * FROM {table_name};"
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                # Получение данных и названий столбцов
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                # Преобразование в DataFrame
                df = pd.DataFrame(rows, columns=columns)
                if 'id' in df.columns:
                    df = df.set_index('id')
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table '{table_name}': {e}")

    def get_token_history(self, token):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT * FROM market_data WHERE token = %s", (token,))
                # Получение данных и названий столбцов
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                # Преобразование в DataFrame
                df = pd.DataFrame(rows, columns=columns)#.set_index('token')
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table: {e}")


    def get_auto_copy_trigger_state(self):
        query = """
            SELECT tgenabled
            FROM pg_trigger
            JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid
            WHERE pg_class.relname = 'current_data'
                AND pg_trigger.tgname = 'after_current_data_change';
        """
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query)
            result = cur.fetchone()

        if result:
            state = result[0]
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
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
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
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            return columns

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
