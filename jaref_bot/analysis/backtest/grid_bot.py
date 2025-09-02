import polars as pl

class GridBotBacktest:
    """    Класс для бектеста grid-бота на фьючерсах.    """

    def __init__(self, exchange, token_name, df, mode, min_price, max_price, n_grids, usdt_amount, leverage,
                 price_scale, ct_val, market_order_fee, limit_order_fee, close_trades = True,
                 take_profit=None, continue_after_tp=None,
                 order_size=None, verbose=False, verbose_deals=False):
        self.df = df.clone()
        self.exchange = exchange
        self.token_name = token_name
        self.mode = mode
        self.min_price = min_price
        self.max_price = max_price
        self.n_grids = n_grids
        self.price_scale = price_scale
        self.ct_val = ct_val
        self.usdt_amount = usdt_amount
        self.leverage = leverage
        self.market_order_fee = market_order_fee
        self.limit_order_fee = limit_order_fee
        self.verbose = verbose
        self.verbose_deals = verbose_deals
        self.take_profit = take_profit
        self.continue_after_tp = continue_after_tp
        self.close_trades = close_trades

        # Создание сетки и размера ордера
        self.grid = self._create_grid()
        self.order_size = self._calculate_order_size() if not order_size else order_size

        # Состояние бота
        self.tp_activated = False
        self.initial_purchase = 0
        self.last_price = 0
        self.open_positions = {}  # Список открытых позиций
        self.trades_history = pl.DataFrame()   # История сделок

    def _create_df(self, ohlcv_df, fund_hist):
        return ohlcv_df.join(fund_hist, how='left').fillna(0)

    def _calculate_buy_and_hold(self):
        open_price = float(self.df[0, 'price'])
        close_price = float(self.df[-1,'price'])

        n_contr = round(self.usdt_amount * self.leverage / open_price / self.ct_val)
        usdt_open = n_contr * self.ct_val * open_price
        open_fee = usdt_open * self.limit_order_fee

        usdt_close = n_contr * self.ct_val * close_price
        close_fee = usdt_close * self.limit_order_fee

        profit = usdt_close - usdt_open - open_fee - close_fee
        return profit

    def _create_grid(self):
        """Создаёт уровни сетки с округлением до указанной точности."""
        grid = [self.min_price]
        price = self.min_price
        if self.exchange == 'gate':
            grid_space = (self.max_price - self.min_price) / (self.n_grids - 1) //( 0.1**self.price_scale) / (10**self.price_scale)
        elif self.exchange == 'bybit':
            grid_space = round((self.max_price - self.min_price) / (self.n_grids), self.price_scale)

        for i in range(self.n_grids - 1):
            price = round(price + grid_space, self.price_scale)
            grid.append(price)
        return grid

    def _calculate_order_size(self):
        """Рассчитывает количество контрактов на один ордер."""
        if self.exchange == 'gate':
            av_price = (self.min_price + self.max_price) / 2
        elif self.exchange == 'bybit':
            av_price = self.max_price

        total_contracts = int(self.usdt_amount * self.leverage / (av_price * self.ct_val))
        contracts_per_order = int(total_contracts / self.n_grids)
        return contracts_per_order

    def _calculate_fee(self, price: float, is_market: bool = False) -> float:
        fee_rate = self.market_order_fee if is_market else self.limit_order_fee
        return price * self.order_size * self.ct_val * fee_rate

    def _open_position(self, date, grid_price, price, is_market=False):
        side = 'sell' if self.mode == 'short' else 'buy'
        fee = self._calculate_fee(price=price, is_market=is_market)
        self.open_positions[grid_price] = {'date': date, 'size': self.order_size, 'price': price, 'fee': fee}
        self.last_price = price

        if self.verbose_deals:
            print(f'{date}: {side} {self.order_size} {self.token_name} on level {price} with fee {fee:.6f}')

    def _close_position(self, date, grid_price, price, is_market=False):
        # side = 'sell' if self.mode == 'long' else 'buy'
        open_grid, open_pos = self.open_positions.popitem()
        open_price = open_pos['price']
        open_size = open_pos['size']
        open_fee = open_pos['fee']
        open_date = open_pos['date']

        close_fee = self._calculate_fee(price=price, is_market=is_market)

        if self.mode == 'long':
            pnl = open_size * self.ct_val * (price - open_price) - (open_fee + close_fee)
        elif self.mode == 'short':
            pnl = open_size * self.ct_val * (open_price - price) - (open_fee + close_fee)

        tdf = pl.DataFrame({
            'open_date': open_date,
            'close_date': date,
            'side': self.mode,
            'open_price': open_price,
            'close_price': price,
            'size': open_size,
            'open_fee': open_fee,
            'close_fee': close_fee,
            'pnl': pnl,
            'type': 'trade'
        })

        self.last_price = price

        self.trades_history = self.trades_history.vstack(tdf)

        if self.verbose_deals:
            print(f'{date}: open: {open_price}; close: {price}; size: {open_size}; PnL: {pnl:.6f}; fees: {open_fee + close_fee:.6f}')

    def _close_all_positions(self, row):
        price = row['price']
        size = row['size']

        if self.mode == 'long':
            close_levels = list(self.open_positions.keys())

            for grid_level in close_levels:
                self._close_position(date=row['date'],
                                        grid_price=grid_level,
                                        price=price,
                                        is_market=True)


    def _calculate_initial_liquidation_price(self, price):
        """        Расчет цены ликвидации для текущих позиций.        """
        mmr = 0.01  # Maintenance Margin Rate = 1%

        if self.mode == 'long':
            return round(price - ((price / self.leverage) * (1 + mmr)), self.price_scale)
        elif self.mode == 'short':
            return round(price + ((price / self.leverage) * (1 + mmr)), self.price_scale)
        return None

    def _apply_funding(self, row):
        """Начисляет/списывает funding по открытым позициям"""
        if not self.open_positions:
            return

        if row['funding'] != 0:
            total_contracts = len(self.open_positions) * self.order_size
            amount = total_contracts * self.ct_val * row['funding'] * row['price']

            tdf = pl.DataFrame({
                'open_date': row['date'],
                'close_date': row['date'],
                'side': self.mode,
                'open_price': 0.0,
                'close_price': 0.0,
                'size': total_contracts,
                'open_fee': 0.0,
                'close_fee': 0.0,
                'pnl': -amount,
                'type': 'funding'
            })

            self.trades_history = self.trades_history.vstack(tdf)

    def _initialize_bot(self, initial_price, initial_time):
        if self.verbose:
            print(f'Время начала: {initial_time}')
            print(f'Начальная цена: {initial_price}')

        if self.mode == 'long':
            for grid_price in self.grid[::-1]:
                if grid_price > initial_price:
                    self.initial_purchase += 1
                    self._open_position(date=initial_time,
                                        grid_price=grid_price,
                                        price=initial_price,
                                        is_market=True)
        elif self.mode == 'short':
            for grid_price in self.grid:
                if grid_price < initial_price:
                    self.initial_purchase += 1
                    self._open_position(date=initial_time,
                                        grid_price=grid_price,
                                        price=initial_price,
                                        is_market=True)

    def _process_row(self, row):
        price = row['price']
        size = row['size']

        # Проверяем выход за границы диапазона
        if price < self.min_price:
            print('STOP-LOSS!!', row['date'])
            return 1
        elif price > self.max_price:
            # print('Цена вышла за границы диапазона!', row['date'])
            pass

        self._apply_funding(row)

        if self.mode == 'long':
            open_levels = [x for x in self.grid if (x >= price and
                                                   size >= self.order_size and
                                                   x not in self.open_positions.keys() and
                                                   x < self.last_price - round(0.1 ** self.price_scale, self.price_scale))]
            close_levels = [x for x in self.open_positions.keys() if (x <= price and
                                                         size >= self.order_size and
                                                         x > self.last_price + round(0.1 ** self.price_scale, self.price_scale))]
            for grid_level in open_levels:
                self._open_position(date=row['date'],
                                        grid_price=grid_level,
                                        price=grid_level,
                                        is_market=False)
            for grid_level in close_levels:
                self._close_position(date=row['date'],
                                        grid_price=grid_level,
                                        price=grid_level,
                                        is_market=False)
        elif self.mode == 'short':
            pass

    def run(self):
        initial_price = float(self.df[0, 'price'])
        liq_price = self._calculate_initial_liquidation_price(initial_price)

        if (self.mode == 'long' and liq_price > self.min_price) or (self.mode == 'short' and liq_price < self.max_price):
            print(f'Цена ликвидации ({liq_price=}) находится в диапазоне сетки!')
            return

        if self.verbose:
            print("Запуск бектеста grid-бота...")
            print(f'Buy and Hold Profit: {self._calculate_buy_and_hold():.2f}')
            print(f"Режим: {self.mode}")
            print(f"Диапазон цен: {self.min_price} - {self.max_price}")
            print(f"Количество сеток: {self.n_grids}")
            print(f'Плечо: {self.leverage}')
            print(f"Размер ордера: {self.order_size} контрактов")
            print(f'Цена ликвидации: {liq_price}')
            print()

        # Открытие начальной позиции
        initial_time = self.df[0, 'date']
        self._initialize_bot(initial_price, initial_time)

        _qty = len(self.open_positions)
        if self.verbose:
            print(f'Открытие позиции: {_qty} orders х {self.order_size} contracts = {_qty * self.order_size} contracts')
            print('======== Торговля ==========')

        # Основной цикл обработки данных
        for row in self.df[self.initial_purchase : ].iter_rows(named=True): # Пропускаем строки с начальной закупкой
            # Обрабатываем take-profit
            if self.take_profit:
                if row['price'] > self.take_profit:
                    self._close_all_positions(row)
                    self.tp_activated = True

                if self.tp_activated:
                    if self.continue_after_tp:
                        if row['price'] < initial_price:
                            initial_time = row['date']
                            self._initialize_bot(initial_price, initial_time)
                            self.tp_activated = False
                    else:
                        return

            if len(self.open_positions) > 0:
                stop_signal = self._process_row(row)

                if stop_signal:
                    self._close_all_positions(row)
                    return

        # Закрываем оставшиеся позиции
        if self.close_trades:
            self._close_all_positions(row)
