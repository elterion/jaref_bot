import psycopg2
from psycopg2.extras import DictCursor
import pandas as pd
from datetime import datetime, timezone, timedelta

class DBManager:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        self.conn.autocommit = True
    
    def add_order(self, token, long_exchange, short_exchange, long_price, short_price,
                 long_avg_price, short_avg_price, volume, long_fee, short_fee, created_at=None):
        """Добавляет новый ордер в таблицу current_orders"""
        query = """
        INSERT INTO current_orders (token, long_exchange, short_exchange, long_price, short_price,
                                    long_avg_price, short_avg_price, volume, long_fee, short_fee, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Если created_at не передан, использовать текущее время
        if created_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            created_at = datetime.now(Moscow_TZ)
    
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (token, long_exchange, short_exchange, long_price, short_price,
                                       long_avg_price, short_avg_price, volume, long_fee, short_fee, created_at))
                # self.conn.commit()
        except psycopg2.IntegrityError as e:
            self.conn.rollback()
            raise ValueError(f"Order with token '{token}' already exists.")

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

    def close_order(self, token, long_closed_price, long_closed_avg_price,
                short_closed_price, short_closed_avg_price,
                long_closed_fee, short_closed_fee, closed_at=None):
        """Закрывает ордер и переносит его в таблицу trade_history"""
        if closed_at is None:
            Moscow_TZ = timezone(timedelta(hours=3))
            closed_at = datetime.now(Moscow_TZ)
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            # Проверка существования записи
            cur.execute("SELECT * FROM current_orders WHERE token = %s", (token,))
            order = cur.fetchone()
            if not order:
                raise ValueError(f"No order found with token '{token}'.")
    
            # Перенос данных в таблицу trade_history
            cur.execute("""
                INSERT INTO trade_history (token, long_exchange, short_exchange, long_price, short_price,
                                           long_avg_price, short_avg_price, volume, long_fee, short_fee,
                                           long_closed_price, long_closed_avg_price,
                                           short_closed_price, short_closed_avg_price,
                                           long_closed_fee, short_closed_fee, created_at, closed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                order['token'], order['long_exchange'], order['short_exchange'], order['long_price'], order['short_price'],
                order['long_avg_price'], order['short_avg_price'], order['volume'], order['long_fee'], order['short_fee'],
                long_closed_price, long_closed_avg_price, short_closed_price, short_closed_avg_price,
                long_closed_fee, short_closed_fee, order['created_at'], closed_at
            ))
    
            # Удаление записи из current_orders
            cur.execute("DELETE FROM current_orders WHERE token = %s", (token,))
            # self.conn.commit()

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
                # self.conn.commit()
                return df
        except Exception as e:
            self.conn.rollback()
            raise ValueError(f"Failed to fetch data from table '{table_name}': {e}")

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