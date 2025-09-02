from rich.console import Console, Group
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.text import Text

from time import sleep
from datetime import datetime
from decimal import Decimal
import polars as pl
import logging
import pickle
import argparse
from redis import ConnectionError
from collections import deque
from collections import defaultdict
import threading

from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.db.redis_manager import RedisManager
from jaref_bot.config.credentials import host, user, password, db_name
from jaref_bot.trading.functions import handle_position, cancel_order
from jaref_bot.core.exceptions.trading import NoSuchOrderError
from jaref_bot.data.data_functions import is_data_up_to_date

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logging.getLogger('aiohttp').setLevel('ERROR')
logging.getLogger('asyncio').setLevel('ERROR')
logger = logging.getLogger()


console = Console()
log_messages = deque(maxlen=5)

class RichLogHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        msg = self.format(record)
        level = record.levelname.lower()

        if level == "error" or level == "critical":
            style = "red"
        elif level == "warning":
            style = "yellow"
        elif level == "info":
            style = "white"
        elif level == "debug":
            style = "dim"
        else:
            style = "white"

        add_log(msg, style)

def setup_logging():
    handler = RichLogHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)

    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser(description="Мониторинг торгового бота")
    parser.add_argument('--demo', action='store_true', help='Включить демонстрационный режим')
    return parser.parse_args()

# Инициализация подключения к биржам, БД и Redis
def init_resources():
    logger.info('Инициализируем биржи...')
    db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
    postgres_manager = DBManager(db_params)
    redis_price_manager = RedisManager(db_name='orderbooks')
    redis_order_manager = RedisManager(db_name='orders')

    with open("./data/coin_information.pkl", "rb") as f:
        coin_info = pickle.load(f)

    return postgres_manager, redis_price_manager, redis_order_manager, coin_info

def get_header_panel(demo, n_orders):
    if demo:
        demo_note = Text(f"ДЕМО РЕЖИМ", style="bold red")
    else:
        demo_note = Text(f"РЕЖИМ РЕАЛЬНОЙ ТОРГОВЛИ", style="bold red")
    ct = datetime.now().strftime('%H:%M:%S')
    update_time = Text(f"Последнее обновление ордербуков: {ct}", style="bold green")
    positions_header = Text(f"Открытых позиций: {n_orders}", style="bold yellow")

    return Group(demo_note, update_time, positions_header)

def get_positions_table(current_data, current_orders, coin_info):
    table = Table(title="Открытые позиции", expand=True, style="cyan")
    table.add_column("token")
    table.add_column("exc")
    table.add_column("qty")
    table.add_column("usdt")
    table.add_column("side", justify="right")
    table.add_column("open_price")
    table.add_column("curr_price")
    table.add_column("lev")
    table.add_column("real_pnl")
    table.add_column("profit")

    for _, data in current_orders.iterrows():
        exc = data['exchange']
        token = data['token']
        dp = int(coin_info[exc + '_linear'][token]['price_scale'])

        qty = int(data['qty'])
        side = data['order_side']
        realized_pnl = data['realized_pnl']
        open_price = round(data['price'], dp)
        leverage = int(data['leverage'])
        usdt = round(qty * open_price / leverage, 2)

        close_side = 'bidprice_0' if side == 'buy' else 'askprice_0'
        try:
            curr_price = current_data.filter((pl.col('symbol') == token) & (pl.col('exchange') == exc)).select(close_side).item()

            if side == 'buy':
                profit = qty * (Decimal(curr_price) - open_price) + realized_pnl
            else:
                profit = qty * (open_price - Decimal(curr_price)) + realized_pnl
        except ValueError:
            curr_price = 0
            profit = 0

        table.add_row(
            str(token),
            str(exc),
            str(qty),
            str(usdt),
            str(side),
            str(open_price),
            str(curr_price),
            str(leverage),
            str(round(realized_pnl, 3)),
            str(round(profit, 2))
        )


    return table

# Формирование панели с информацией по текущим ордерам
def create_pending_panel(pending_orders):
    pending_lines = []

    for exc, data in pending_orders.items():
        for token, td in data.items():
            order_time = datetime.fromtimestamp(int(td['ts'])).strftime('%H:%M:%S')
            pending_lines.append(f"{order_time}: {td['side']} {td['qty']} {token} on {exc} ({td['status']})")
    pending_text = "\n".join(pending_lines) if pending_lines else "Нет ордеров."

    return Panel(pending_text, title="Текущие ордеры", style="magenta")

# Формирование панели с логами
def render_log_panel() -> Panel:
    lines = []

    for timestamp, msg, style in log_messages:
        line = Text(f"[{timestamp}] {msg}", style=style)
        lines.append(line)

    return Panel(
        Group(*lines),
        title="Лог событий",
        border_style="bright_black",
        padding=(0, 1)
    )

def add_log(message: str, style: str = "white"):
    now = datetime.now().strftime("%H:%M:%S")
    log_messages.append((now, message, style))


# Обработка лимитных заявок: отмена или исполнение
def process_pending_orders(redis_order_manager, pending_orders, coin_info, timeout, demo):
    orders_by_token = defaultdict(lambda: {"buy": set(), "sell": set()})

    ts = int(datetime.timestamp(datetime.now()))
    for exc, data in pending_orders.items():
        exc_name = exc + '_linear'
        for symbol, params in data.items():
            status = params.get('status')

            # Находим в словаре pending_orders все закрытые в обоих направлениях ордеры
            if status == 'filled':
                side = params.get('side')
                orders_by_token[symbol][side].add(exc)

            # Если заявка "placed" и она не исполнилась вовремя, отменяем её
            if status == 'placed':
                if ts - int(params['ts']) > timeout:
                    add_log('Отменяем заявку.')
                    try:
                        cancel_order(demo, exc_name, symbol, params['id'])
                    except NoSuchOrderError:
                        pass
                    # Удаляем заявку из Redis
                    redis_order_manager.delete_order(exchange=exc, token=symbol)
                # Если позиция открылась, обновляем статус заявки
                elif handle_position(demo=demo,
                                    exc=exc_name,
                                    symbol=symbol,
                                    order_type='limit',
                                    coin_information=coin_info):
                    redis_order_manager.add_order(exchange=exc,
                                                  token=symbol,
                                                  qty=params['qty'],
                                                  side=params['side'],
                                                  ord_id=params['id'],
                                                  ts=ts,
                                                  status='filled')

    # Удаляем из pending_orders все полностью выкупленные ордеры
    for token, sides in orders_by_token.items():
        if sides['buy'] and sides['sell']:
            # Объединяем биржи, на которых есть заполненные ордера для данного токена
            exchanges_to_delete = sides['buy'] | sides['sell']
            for exchange in exchanges_to_delete:
                redis_order_manager.delete_order(exchange, token)

# Функция, обновляющая текущие позиции
def update_positions(demo, current_orders, coin_info):
    while True:
        try:
            if not current_orders.empty:
                add_log("Обновление состояния открытых позиций...")

                for _, row in current_orders.iterrows():
                    handle_position(demo=demo,
                                    exc=row['exchange'] + '_linear',
                                    symbol=row['token'],
                                    order_type='limit',
                                    coin_information=coin_info)
        except Exception as e:
            add_log(f"Ошибка при обновлении позиций: {e}", style="bold red")
        sleep(300)

def make_layout():
    layout = Layout()

    layout.split(
        Layout(name="status", size=3),
        Layout(name="positions", ratio=2),
        Layout(name="orders", ratio=1),
        Layout(name="log", ratio=1)
    )
    return layout

def main_loop(demo):
    # Задаём гиперпараметры работы программы
    order_timeout = 20

    # Задаём начальные значения переменных

    postgres_manager, redis_price_manager, redis_order_manager, coin_info = init_resources()

    # Запуск фонового потока для периодического обновления открытых позиций
    current_orders = postgres_manager.get_table('current_orders')
    update_thread = threading.Thread(target=update_positions,
                                     args=(demo, current_orders, coin_info),
                                     daemon=True)
    update_thread.start()

    layout = make_layout()
    with Live(layout, refresh_per_second=5, screen=True):
        while True:
            try:
                current_data = redis_price_manager.get_orderbooks(n_levels=5)
                current_orders = postgres_manager.get_table('current_orders')
                pending_orders = redis_order_manager.get_pending_orders()

                # Проверка на то, что данные являются актуальными, что ни одна из бирж не подвисла
                if not is_data_up_to_date(current_data, time_thresh=10):
                    add_log(f'Устаревшие данные по ценам.', style="bold red")
                    sleep(1)
                if len(current_data) == 0:
                    add_log(f'No data!', style="bold red")
                    sleep(1)

                layout["status"].update(get_header_panel(demo=args.demo, n_orders=len(current_orders)))
                layout["positions"].update(get_positions_table(current_data,
                                                        current_orders,
                                                        coin_info))
                layout["orders"].update(create_pending_panel(pending_orders))
                layout["log"].update(render_log_panel())

                # Обработка лимитных заявок
                process_pending_orders(redis_order_manager,
                                       pending_orders,
                                       coin_info=coin_info,
                                       timeout=order_timeout,
                                       demo=args.demo)

                sleep(0.2)
            except pl.exceptions.ColumnNotFoundError:
                add_log(f'Нет данных!', style="bold red")
                sleep(1)
            except ConnectionError:
                add_log(f'Отсутствует соединение с базой данных!', style="bold red")
                sleep(5)
            except KeyboardInterrupt:
                console.print("Завершение работы.", style="bold red")
                break

if __name__ == '__main__':
    setup_logging()
    args = parse_args()
    main_loop(demo=args.demo)
