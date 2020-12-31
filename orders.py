from datetime import datetime
import asyncio
import queue
import strategy as st
import json
import logging
import logging.config
import logging.handlers
import utilities as util
import random

OrderQ = queue.Queue()
order_book = {}
order_log = []

OBJ_KITE_CONNECT = None
order_status_placed  = 0
order_status_executed = 1
order_type_buy  = 'BUY'
order_type_sell = 'SELL'

ORDER_DEFAULT_QUANTITY = 10
KITE_ORDERS_LIST = None
KITE_POSITIONS = None

def add_to_order_queue(order_obj):
    OrderQ.put(order_obj)


def get_best_market_depth_from_tick(tick,buy_sell):
    # later we can reverse this logic
    try:
        depth = tick['depth']
        if buy_sell == st.buy_signal:
            buy = depth['buy']
            price = 0
            max = 0
            for rec in buy:
                if int(rec['quantity']) > max:
                    max = int(rec['quantity'])
                    price = float(rec['price'])
            return price,max

        if buy_sell == st.sell_signal:
            sell = depth['sell']
            price = 0
            max = 0
            for rec in sell:
                if int(rec['quantity']) > max:
                    max = int(rec['quantity'])
                    price = float(rec['price'])
            return price, max
    except Exception as ex:
        util.lgr_orders.log(logging.ERROR,'exception in get_best_market_depth_from_tick {}'.format(ex))
        return None,None


async def Check_for_Orders_Queue(ticks,cls_order,kite_object):
    cls_order.OBJ_KITE_CONNECT = kite_object
    util.lgr_orders.log(logging.DEBUG,'inside Check_for_Orders_Queue KiteConnect Object ')
    util.lgr_orders.log(logging.DEBUG,OBJ_KITE_CONNECT)
    util.lgr_orders.log(logging.DEBUG,'ticks count {}'.format(len(ticks)))
    # Fetch all orders
    orders_list = cls_order.OBJ_KITE_CONNECT.orders()
    positions_list = cls_order.OBJ_KITE_CONNECT.positions()

    token_dict = {}
    for tick in ticks:
        instrument = tick["instrument_token"]
        token_dict[instrument] = tick

    temp_queue = queue.Queue()
    try:
        ord_obj = cls_order.OrderQ.get(False)
        if ord_obj is not None:
            util.lgr_orders.log(logging.DEBUG, 'found order object')
            util.lgr_orders.log(logging.DEBUG, ord_obj)

        if ord_obj['instrument'] in token_dict.keys():
            tk = token_dict[ord_obj['instrument']]
            n_ord_obj_price = int(ord_obj['price'])
            n_tk_ltp = int(tk["last_price"])

            if ord_obj['order_type'] == st.buy_signal:
                util.lgr_orders.log(logging.DEBUG, 'placing buy order')
                if n_ord_obj_price == n_tk_ltp:
                    price,best_depth = get_best_market_depth_from_tick(tick,st.buy_signal)
                    if price is not None and best_depth is not None:
                        buy_order_present = cls_order.check_if_buy_order_is_present(ord_obj['instrument'], price,orders_list,positions_list)
                        if not buy_order_present:
                            util.lgr_orders.log(logging.DEBUG,'{} placing buy order at {}'.format(ord_obj['instrument'], price))
                            cls_order.place_buy_limit_order(p_price=price,p_tradingSymbol=ord_obj['instrument'],p_quantity=ORDER_DEFAULT_QUANTITY)
                        else:
                            util.lgr_orders.log(logging.DEBUG, '{} buy order already present at {}'.format(ord_obj['instrument'],price))

            if ord_obj['order_type'] == st.sell_signal:
                if n_ord_obj_price == n_tk_ltp:
                    util.lgr_orders.log(logging.DEBUG, 'placing sell order')
                    price, best_depth = get_best_market_depth_from_tick(tick, st.buy_signal)
                    if price is not None and best_depth is not None:
                        sell_order_present = cls_order.check_if_sell_order_is_present(ord_obj['instrument'],price,orders_list,positions_list)
                        if not sell_order_present:
                            util.lgr_orders.log(logging.DEBUG,
                                                '{} placing sell order at {}'.format(ord_obj['instrument'], price))
                            cls_order.place_sell_limit_order(p_price=price,p_tradingSymbol=ord_obj['instrument'],p_quantity=ORDER_DEFAULT_QUANTITY)
                        else:
                            util.lgr_orders.log(logging.DEBUG,'{} sell order already present at {}'.format(ord_obj['instrument'], price))
        else:
            util.lgr_orders.log(logging.DEBUG, "ord_obj['instrument'] not present in token_dict.keys():")
            util.lgr_orders.log(logging.DEBUG, "ord_obj['instrument'] {} token_dict keys {}".format(ord_obj['instrument'],token_dict.keys()))
            temp_queue.put(ord_obj)

    except queue.Empty:
        return False

    cls_order.OrderQ = temp_queue
    write_dict_as_json('order_book.json',cls_order.order_book)
    util.lgr_orders.log(logging.DEBUG,'order book json written')
    return temp_queue


def add_buy_order_to_book(order_id,tr_symbol,quantity,price):
    try:
        total_price = int(quantity) * float(price)
        str_datetime = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        rec = {order_id:{'quantity': quantity, 'price': price, 'total_price': total_price, 'type': order_type_buy,
                'status':order_status_placed,'time': str_datetime}}
        util.lgr_orders.log(logging.DEBUG,'writing buy order to order_book {}'.format(rec))
        if tr_symbol not in order_book.keys():
            order_book[tr_symbol] = [rec]
        else:
            order_book[tr_symbol].append(rec)
    except Exception as ex:
        util.lgr_orders.log(logging.ERROR, 'exception in add_buy_order_to_book {}'.format(ex))


def add_sell_order_to_book(order_id,tr_symbol,quantity,price):
    try:
        total_price = int(quantity) * float(price)
        str_datetime = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        rec = {order_id: {'quantity': quantity, 'price': price, 'total_price': total_price, 'type': order_type_sell,
                          'time': str_datetime}}
        util.lgr_orders.log(logging.DEBUG, 'writing sell order to order_book {}'.format(rec))
        if tr_symbol not in order_book.keys():
            order_book[tr_symbol] = [rec]
        else:
            order_book[tr_symbol].append(rec)
    except Exception as ex:
        util.lgr_orders.log(logging.ERROR, 'exception in add_sell_order_to_book {}'.format(ex))


def place_buy_limit_order(p_tradingSymbol,p_quantity,p_price):
    try:
        order_id = random.randint(0,500)
        if OBJ_KITE_CONNECT is None:
            util.lgr_orders.log(logging.DEBUG,'cannot place buy order as OBJ_KITE_CONNECT is None')
            return
        variety = 'regular'
        exchange = 'NSE'
        tradingsymbol = p_tradingSymbol
        transaction_type = 'BUY'
        quantity = p_quantity
        product = 'CNC'
        order_type = 'LIMIT'
        price = p_price
        validity = 'DAY'
        # place_order(variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None,
        # validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
        order_id = OBJ_KITE_CONNECT.place_order(variety, exchange, tradingsymbol, transaction_type, quantity, product,
                                                order_type, price, validity)
    except Exception as ex:
        util.lgr_orders.log(logging.DEBUG, 'exception in place_buy_limit_order {}'.format(ex))

    add_buy_order_to_book(order_id,p_tradingSymbol,p_quantity,p_price)


def place_sell_limit_order(p_tradingSymbol,p_quantity,p_price):
    #place sell order only if there is corresponding buy order
    try:
        if OBJ_KITE_CONNECT is None:
            util.lgr_orders.log(logging.DEBUG,'cannot place sell order as OBJ_KITE_CONNECT is None')
            return
        variety = 'regular'
        exchange = 'NSE'
        tradingsymbol = p_tradingSymbol
        transaction_type = 'SELL'
        quantity = p_quantity
        product = 'CNC'
        order_type = 'LIMIT'
        price = p_price
        validity = 'DAY'
        #place_order(variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None, validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None)
        order_id = OBJ_KITE_CONNECT.place_order(variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price, validity)
    except Exception as ex:
        util.lgr_orders.log(logging.ERROR, 'ERROR in place_sell_limit_order {}'.format(ex))

    add_sell_order_to_book(order_id,p_tradingSymbol,p_quantity,p_price)


def change_order_status(order_id,tradingSymbol,status):
    rec = {'time':datetime.now(),'symbol':tradingSymbol,'order_id':order_id,'status':status}
    order_log.append(rec)


def check_if_buy_order_is_present(tradingSymbol, price,orders_list,positions_list):
    if len(orders_list) == 0 or len(positions_list) == 0:
        return False

    for rec in orders_list:
        if rec['tradingsymbol'] == tradingSymbol and rec['transaction_type'] == 'BUY':
            if rec['status'] != 'COMPLETE':
                return True
            if rec['status'] == 'COMPLETE' and rec['price'] == price:
                return True

    return False


def completed_buy_order_present(tradingSymbol, price,orders_list,positions_list):
    if len(orders_list) == 0 or len(positions_list) == 0:
        return False

    for rec in orders_list:
        if rec['tradingsymbol'] == tradingSymbol and rec['transaction_type'] == 'BUY':
            if rec['status'] == 'COMPLETE':
                return True

    return False


def completed_sell_order_present(tradingSymbol, price,orders_list,positions_list):
    if len(orders_list) == 0 or len(positions_list) == 0:
        return False

    for rec in orders_list:
        if rec['tradingsymbol'] == tradingSymbol and rec['transaction_type'] == 'SELL':
            if rec['status'] == 'COMPLETE':
                return True

    return False


def check_if_sell_order_is_present(tradingSymbol, price,orders_list,positions_list):

    if len(orders_list) == 0 or len(positions_list) == 0:
        return False

    for rec in orders_list:
        if rec['tradingsymbol'] == tradingSymbol and rec['transaction_type'] == 'SELL':
            if rec['status'] != 'COMPLETE':
                return True
            if rec['status'] == 'COMPLETE' and rec['price'] == price:
                return True

    return False


def update_order_status_in_orderbook(tradingSymbol,order_id,status):
    try:
        order_book[tradingSymbol][order_id]['status'] = status
    except Exception as ex:
        util.lgr_orders.log(logging.DEBUG,'exception in update_order_status_in_orderbook')
        util.lgr_orders.log(logging.DEBUG,ex)


def write_dict_as_json(filename,dict_to_write):
    util.lgr_orders.log(logging.DEBUG,'writing order book as json')
    util.lgr_orders.log(logging.DEBUG,dict_to_write)
    with open(filename, 'w') as file:
        json_string = json.dumps(dict_to_write, default=lambda o: o.__dict__, sort_keys=True, indent=2)
        file.write(json_string)