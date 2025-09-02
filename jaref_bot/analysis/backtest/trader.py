import polars as pl
import numpy as np

class Trader():
    def __init__(self, sym_1, sym_2, balance, order_size, leverage, fee_rate, dp_1, dp_2):
        self.sym_1 = sym_1
        self.sym_2 = sym_2
        self.balance = balance
        self.order_size = order_size
        self.leverage = leverage
        self.fee_rate = fee_rate
        self.dp_1 = dp_1
        self.dp_2 = dp_2

        self._trades_buffer = []
        self._set_environment()

    def _set_environment(self):
        self.time_open = None
        self.qty_1 = 0
        self.qty_2 = 0
        self.open_price_1 = None
        self.open_price_2 = None
        self.pos_side = None

    def _round(self, value, dp):
        return np.floor(value / dp) * dp

    def _calculate_qty(self, price_1: float, price_2: float) -> tuple:
        qty_1 = self._round(self.leverage * self.order_size /
                            (1 + 2 * self.fee_rate) / price_1,
                            self.dp_1)
        qty_2  = self._round(self.leverage * self.order_size /
                             (1 + 2 * self.fee_rate) / price_2,
                             self.dp_2)

        return qty_1, qty_2

    def open_position(self, time, price_1, price_2, pos_side: str):
        # Сначала найдём размер позиции для каждой монеты в зависимости от beta
        self.time_open = time
        self.qty_1, self.qty_2 = self._calculate_qty(price_1, price_2)
        self.open_price_1 = price_1
        self.open_price_2 = price_2
        self.pos_side = pos_side

    def close_position(self, time, price_1, price_2, reason):
        usdt_open_1 = self.qty_1 * self.open_price_1
        usdt_open_2 = self.qty_2 * self.open_price_2

        open_fee_1 = usdt_open_1 * self.fee_rate
        open_fee_2 = usdt_open_2 * self.fee_rate

        usdt_close_1 = self.qty_1 * price_1
        usdt_close_2 = self.qty_2 * price_2

        close_fee_1 = usdt_close_1 * self.fee_rate
        close_fee_2 = usdt_close_2 * self.fee_rate

        if self.pos_side == f'{self.sym_1}_long':
            profit_1 = usdt_close_1 - usdt_open_1 - open_fee_1 - close_fee_1
            profit_2 = usdt_open_2 - usdt_close_2 - open_fee_2 - close_fee_2
        elif self.pos_side == f'{self.sym_1}_short':
            profit_1 = usdt_open_1 - usdt_close_1 - open_fee_1 - close_fee_1
            profit_2 = usdt_close_2 - usdt_open_2 - open_fee_2 - close_fee_2

        self._trades_buffer.append({
            'open_time': self.time_open,
            'close_time': time,
            f'{self.sym_1}_open': self.open_price_1,
            f'{self.sym_1}_close': price_1,
            f'{self.sym_2}_open': self.open_price_2,
            f'{self.sym_2}_close': price_2,
            'side': self.pos_side,
            'fees': open_fee_1 + open_fee_2 + close_fee_1 + close_fee_2,
            f'{self.sym_1}_profit': profit_1,
            f'{self.sym_2}_profit': profit_2,
            'profit': profit_1 + profit_2,
            'reason': reason
        })
        self._set_environment()

    def liquidate_position(self, time, price_1, price_2, liq_side):
        usdt_open_1 = self.qty_1 * self.open_price_1
        usdt_open_2 = self.qty_2 * self.open_price_2

        open_fee_1 = usdt_open_1 * self.fee_rate
        open_fee_2 = usdt_open_2 * self.fee_rate

        usdt_close_1 = self.qty_1 * price_1
        usdt_close_2 = self.qty_2 * price_2

        close_fee_1 = usdt_close_1 * self.fee_rate
        close_fee_2 = usdt_close_2 * self.fee_rate

        if self.pos_side == f'{self.sym_1}_long':
            profit_1 = usdt_close_1 - usdt_open_1 - open_fee_1 - close_fee_1
            profit_2 = usdt_open_2 - usdt_close_2 - open_fee_2 - close_fee_2
        elif self.pos_side == f'{self.sym_1}_short':
            profit_1 = usdt_open_1 - usdt_close_1 - open_fee_1 - close_fee_1
            profit_2 = usdt_close_2 - usdt_open_2 - open_fee_2 - close_fee_2

        if liq_side == 'long':
            profit_1 = -usdt_open_1
            close_fee_1 = 0
        elif liq_side == 'short':
            profit_2 = -usdt_open_2
            close_fee_2 = 0

        self._trades_buffer.append({
            'open_time': self.time_open,
            'close_time': time,
            f'{self.sym_1}_open': self.open_price_1,
            f'{self.sym_1}_close': price_1,
            f'{self.sym_2}_open': self.open_price_2,
            f'{self.sym_2}_close': price_2,
            'side': self.pos_side,
            'fees': open_fee_1 + open_fee_2 + close_fee_1 + close_fee_2,
            f'{self.sym_1}_profit': profit_1,
            f'{self.sym_2}_profit': profit_2,
            'profit': profit_1 + profit_2,
            'reason': 'liquidation'
        })
        self._set_environment()


    def get_trades_history(self):
        return pl.DataFrame(self._trades_buffer) if self._trades_buffer else pl.DataFrame()
