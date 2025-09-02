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

from jaref_bot.db.redis_manager import RedisManager
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
    # positions_header = Text(f"Открытых позиций: {n_orders}", style="bold yellow")

    return Group(demo_note, update_time)

def get_main_table(current_data, coin_info):
    table = Table(title="Торговые пары", expand=True, style="cyan")
    table.add_column("token_1")
    table.add_column("token_2")
    table.add_column("spread")
    table.add_column("mean")
    table.add_column("std")
    table.add_column("z_score")

    xai = current_data.filter(pl.col('symbol') == 'XAI_USDT')
    strk = current_data.filter(pl.col('symbol') == 'STRK_USDT')

    token_1 = xai['']

    table.add_row(
        str(token_1),
        str(token_2),
        str(spread),
        str(mean),
        str(std),
        str(z_score)
    )


    return table

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

def make_layout():
    layout = Layout()

    layout.split(
        Layout(name="status", size=3),
        Layout(name="main", ratio=2),
        Layout(name="log", ratio=1)
    )
    return layout

def main_loop(demo):
    # Задаём начальные значения переменных
    postgres_manager, redis_price_manager, redis_order_manager, coin_info = init_resources()

    # Запуск фонового потока для периодического обновления открытых позиций
    current_orders = postgres_manager.get_table('current_orders')

    layout = make_layout()
    with Live(layout, refresh_per_second=1, screen=True):
        while True:
            try:
                current_data = redis_price_manager.get_orderbooks(n_levels=1)

                # Проверка на то, что данные являются актуальными, что ни одна из бирж не подвисла
                if not is_data_up_to_date(current_data, time_thresh=10):
                    add_log(f'Устаревшие данные по ценам.', style="bold red")
                    sleep(1)
                if len(current_data) == 0:
                    add_log(f'No data!', style="bold red")
                    sleep(1)

                layout["status"].update(get_header_panel(demo=args.demo, n_orders=len(current_orders)))
                layout["main"].update(get_main_table(current_data,
                                                        current_orders,
                                                        coin_info))
                layout["log"].update(render_log_panel())


                sleep(0.5)
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
