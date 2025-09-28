from decimal import Decimal
from datetime import datetime
from jaref_bot.trading.trade_api import BybitTrade, OkxTrade, GateTrade
from jaref_bot.db.postgres_manager import DBManager
from jaref_bot.config.credentials import host, user, password, db_name

db_params = {'host': host, 'user': user, 'password': password, 'dbname': db_name}
postgre_manager = DBManager(db_params)

def set_leverage(demo, exc, symbol, leverage):
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc = exc_dict[exchange](demo=demo)
    resp_lever = exc.set_leverage(market_type=market_type, symbol=symbol, lever=leverage)

def place_market_order(demo, exc, symbol, side, volume, coin_information, stop_loss=None):
    # Мы получаем уже обработанное значение volume, то есть мы уже проверили округление для всех бирж
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc_manager = exc_dict[exchange](demo=demo)

    ct_val = coin_information[exc][symbol]['ct_val']
    resp_order = exc_manager.place_market_order(market_type='linear',
                                                symbol=symbol,
                                                side=side,
                                                qty=volume,
                                                stop_loss=stop_loss,
                                                ct_val=ct_val)

    return resp_order

def place_limit_order(demo: bool,
                      exc: str,
                      symbol: str,
                      side: str,
                      volume: Decimal,
                      price: float,
                      coin_information: dict,
                      stop_loss: float = None) -> str:
    # Мы получаем уже обработанное значение volume, то есть мы уже проверили округление для всех бирж
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc_manager = exc_dict[exchange](demo=demo)

    ct_val = coin_information[exc][symbol]['ct_val']
    resp_order = exc_manager.place_limit_order(market_type='linear',
                                               symbol=symbol,
                                               side=side,
                                               qty=volume,
                                               price=price,
                                               stop_loss=stop_loss,
                                               ct_val=ct_val)

    return resp_order

def cancel_order(demo, exc, symbol, order_id):
    # Мы получаем уже обработанное значение volume, то есть мы уже проверили округление для всех бирж
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc_manager = exc_dict[exchange](demo=demo)

    resp_order = exc_manager.cancel_order(market_type=market_type, symbol=symbol, order_id=order_id)

    return resp_order

def handle_opened_position(demo, exc, symbol, order_type, coin_information):
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc_manager = exc_dict[exchange](demo=demo)

    ct_val = coin_information[exc][symbol]['ct_val']

    pos = exc_manager.get_position(market_type='linear', symbol=symbol, order_type=order_type, ct_val=ct_val)
    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if pos:
        if pos['qty'] > 0 and pos['usdt_amount'] > 0:
            postgre_manager.place_order(token=symbol, order_type=order_type, order_side=pos['order_side'],
                    qty=abs(pos['qty']), price=pos['price'], usdt_amount=pos['usdt_amount'],
                    realized_pnl=pos['realized_pnl'], leverage=pos['leverage'])
            print(f'[{ct}] [OPENED] {pos['order_side']} {pos['qty']} {symbol[:-5]} for {pos['usdt_amount']:.4f} (price: {pos['price']}) on {exchange}; leverage: {pos['leverage']}')
            return True
        elif pos['qty'] == 0:
            postgre_manager.delete_order(token=symbol)
        else:
            return pos
    else:
        postgre_manager.delete_order(token=symbol)

def handle_close_position(demo, resp, exc, symbol, order_type, leverage, coin_information):
    exc_dict = {'bybit': BybitTrade, 'okx': OkxTrade, 'gate': GateTrade}
    exchange, market_type = exc.split('_')
    exc_manager = exc_dict[exchange](demo=demo)

    ct_val = coin_information[exc][symbol]['ct_val']
    price_scale = coin_information[exc][symbol]['price_scale']

    pos = exc_manager.get_order(symbol=symbol, market_type='linear', order_id=resp, order_type=order_type, ct_val=ct_val)
    usdt_amount = round(pos['usdt_amount'] / leverage, price_scale)
    ct = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if pos:
        postgre_manager.close_order(token=symbol,
                                    qty=abs(pos['qty']),
                                    close_price=pos['price'],
                                    close_usdt_amount=usdt_amount,
                                    close_fee=pos['fee'])
        print(f'[{ct}] [CLOSED] {pos['order_side']} {pos['qty']} {symbol[:-5]} for {usdt_amount:.4f} (price: {pos['price']}) on {exchange}')
