import schedule
import time
from typing import Literal 
from datetime import datetime as dt, timedelta
from exchange.koreainvestment import KoreaInvestment
from util.logger import StrategyLogger

class DisparityArbitragy:
    def __init__(self, positive_etf_code, inverse_etf_code, base_disparity, min_cash=0, max_cash=2_000_000) -> None:
        self._positive_etf_code = positive_etf_code
        self._inverse_etf_code = inverse_etf_code
        self._base_disparity = base_disparity
        self._min_cash = min_cash
        self._max_cash = max_cash
        self._logger = StrategyLogger(f'{self.__class__.__name__}_{positive_etf_code}').logger
        self._broker = KoreaInvestment()

    def on_trading_iteration(self) -> None:
        self._logger.info(f'Start trading iteration: exchange=KoreaInvestment, \
positive_etf: {self._positive_etf_code}, \
inverse_etf={self._inverse_etf_code}, \
base_disparity={self._base_disparity} \
min_cash={self._min_cash}, \
max_cash={self._max_cash}')
        
        positive_etf_position = self._broker.fetch_open_position(self._positive_etf_code)
        inverse_etf_position = self._broker.fetch_open_position(self._inverse_etf_code)
        
        self.positive_etf_price = self._broker.fetch_etf_price(self._positive_etf_code)
        self.inverse_etf_price = self._broker.fetch_etf_price(self._inverse_etf_code)
        
        positive_etf_orderbook = self._broker.fetch_orderbook(self._positive_etf_code)
        inverse_etf_orderbook = self._broker.fetch_orderbook(self._inverse_etf_code)
        
        positive_buy_avg_price, positive_buy_disparity_rate = self._calculate_average_disparity(positive_etf_orderbook, self.positive_etf_price.nav, 'buy')
        inverse_buy_avg_price, inverse_buy_disparity_rate = self._calculate_average_disparity(inverse_etf_orderbook, self.inverse_etf_price.nav, 'buy')
        buy_disparity_sum = positive_buy_disparity_rate + inverse_buy_disparity_rate
        self._logger.info(f'current positive_avg_price: {positive_buy_avg_price}, inverse_avg_price: {inverse_buy_avg_price}, disparity_sum: {buy_disparity_sum}')
        
        # buy signal
        if positive_etf_position == None \
            and self._is_time_near_market_closing() \
            and buy_disparity_sum <= self._base_disparity:  
                self._logger.info(f'buy signal is up')
                positive_order_size, inverse_order_size, _ = self._calculate_order_size(
                    positive_buy_avg_price,
                    inverse_buy_avg_price,
                    self._min_cash,
                    self._max_cash)
                result = self._submit_order_etf_pair(self._positive_etf_code, positive_order_size, self._inverse_etf_code, inverse_order_size, 'buy', 0, 'market')        
        # sell signal
        elif positive_etf_position != None and positive_etf_position.size > 0:
            _, positive_sell_disparity_rate = self._calculate_average_disparity(positive_etf_orderbook, self.positive_etf_price.nav, 'sell')
            _, inverse_sell_disparity_rate = self._calculate_average_disparity(inverse_etf_orderbook, self.inverse_etf_price.nav, 'sell')
            sell_disparity_sum = positive_sell_disparity_rate + inverse_sell_disparity_rate
            if sell_disparity_sum > 0:
                self._logger.info(f'sell signal is up')
                result = self._submit_order_etf_pair(self._positive_etf_code, positive_etf_position.size, self._inverse_etf_code, inverse_etf_position.size, 'sell', 0, 'market')
            # reblancing positions
            elif self._is_time_near_market_closing():
                total_amount = positive_etf_position.size * self.positive_etf_price.current_price + inverse_etf_position.size * self.inverse_etf_price.current_price
                positive_order_size, inverse_order_size, _ = self._calculate_order_size(
                    positive_buy_avg_price,
                    inverse_buy_avg_price,
                    min_cash=total_amount,
                    max_cash=self._max_cash)
                if positive_order_size == 0:
                    self._logger.error(f'Cannot buy more etf pair for rebalancing')
                    result = self._submit_order_etf_pair(self._positive_etf_code, positive_etf_position.size, self._inverse_etf_code, inverse_etf_position.size, 'sell', 0, 'market')
                else:
                    self._logger.info(f'buy more etf pair for rebalancing')
                    result = self._submit_order_etf_pair(
                        self._positive_etf_code, 
                        positive_order_size - positive_etf_position.size,
                        self._inverse_etf_code,
                        inverse_order_size - inverse_etf_position.size,
                        0, 'market')

    def _submit_order_etf_pair(self, positive_etf_code, positive_order_size, inverse_etf_code, inverse_order_size, side, price, order_type) -> tuple:
        positive_order = {
            "code": positive_etf_code,
            "side": side,
            "size": positive_order_size,
            "price": price,
            "type": order_type
        }
        inverse_order = {
            "code": inverse_etf_code,
            "side": side,
            "size": inverse_order_size,
            "price": price,
            "type": order_type
        }
        self._logger.info(f'Submit {side} market orders: pos_size={positive_order_size}, pos_price:{self.positive_etf_price.current_price} \
inverse_size={inverse_order_size}, inverse_price={self.inverse_etf_price.current_price}')
        return self._broker.submit_order(positive_order), self._broker.submit_order(inverse_order)

    def _calculate_average_disparity(self, orderbook, nav, side=Literal['buy', 'sell']) -> tuple:
        if side == 'buy':
            order_side = 'askp'
        elif side == 'sell':
            order_side = 'bidp'
        else:
            raise ValueError(f'Invalid side: {side}')
        max_available = self._max_cash / 2
        remain_cash = max_available
        total_qty = 0
        orderbook_price = [f'{order_side}{n}' for n in range(1, 11)]
        orderbook_qty = [f'{order_side}_rsqn{n}' for n in range(1, 11)]
        for price_key, qty_key in zip(orderbook_price, orderbook_qty):
            total_price = orderbook[price_key] * orderbook[qty_key]
            if total_price >= remain_cash:
                if remain_cash == max_available:
                    avg_price = orderbook[price_key]
                else:
                    remain_qty = remain_cash / orderbook[price_key]
                    total_qty += remain_qty
                    avg_price = max_available / total_qty
                break
            else:
                remain_cash -= total_price
                total_qty += orderbook[qty_key]
        return (avg_price, (avg_price - nav) / nav * 100)
    
    def _calculate_order_size(self, positive_price: int, inverse_price: int, min_cash: int, max_cash=3_000_000) -> tuple:
        optimal_positive_size = 0
        optimal_inverse_size = 0
        total_cost_positive = 0
        total_cost_inverse = 0

        min_difference = float('inf')
        max_positive_size = int((max_cash / 2) / positive_price)
        max_inverse_size = int((max_cash / 2) / inverse_price)
        for positive_size in range(1, max_positive_size):
            inverse_size = round(positive_price / inverse_price * positive_size)
            for delta in [0, 1]:
                current_inverse_size = inverse_size + delta
                if current_inverse_size <= 0 or inverse_size > max_inverse_size:
                    continue
                positive_cost = positive_price * positive_size
                inverse_cost = inverse_price * current_inverse_size
                if positive_cost + inverse_cost < min_cash:
                    continue
                difference = abs(positive_cost - inverse_cost)
                if difference < min_difference:
                    min_difference = difference
                    optimal_positive_size = positive_size
                    optimal_inverse_size = current_inverse_size
                    total_cost_positive = positive_cost
                    total_cost_inverse = inverse_cost
        return optimal_positive_size, optimal_inverse_size, abs(total_cost_positive - total_cost_inverse)

    def _is_time_near_market_closing(self):
        return dt.strptime('15:00', '%H:%M').time() < dt.now().time() < dt.strptime('15:20', '%H:%M').time()

if __name__ == '__main__':
    # kodex nasdaq 100(H), kodex nasdaq 100 inverse
    disparity_arb = DisparityArbitragy('304940', '409810', base_disparity=-0.15, min_cash=0, max_cash=2_000_000)
    
    start_time = dt.now().replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = dt.now().replace(hour=15, minute=20, second=0, microsecond=0)
    current_time = start_time
    while current_time <= end_time:
        schedule.every().day.at(current_time.strftime("%H:%M")).do(disparity_arb.on_trading_iteration)
        current_time += timedelta(minutes=10)
    while True:
        schedule.run_pending()
        time.sleep(1)