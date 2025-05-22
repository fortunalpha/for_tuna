import os
import pandas as pd
import numpy as np
import pandas_ta as ta
import ccxt
import schedule
import time
from dotenv import load_dotenv
from util.logger import StrategyLogger
from pprint import pprint
from math import ceil

# one-way future mode
class BollingerbandBreakout:
    def __init__(self, base, quote, prio, pyramiding=2, interval='4h', leverage=1, margin_mode='isolated'):
        self.base = base
        self.quote = quote
        self.prio = prio
        self.pyramiding = pyramiding
        self.interval = interval                                                                                                                                                                                                                                 
        self.leverage = leverage
        self.margin_mode = margin_mode
        # self.execution_times = ['01:00', '05:00', '09:00', '13:00', '17:00', '21:00']
        self._logger = StrategyLogger(f'{self.__class__.__name__}_{self.base}').logger
        
        load_dotenv()
        api_key = os.getenv('BINANCE_ACCESS_KEY')
        api_secret = os.getenv('BINANCE_SECRET_KEY')
        self.exchange = ccxt.binance(config={
            'apiKey': api_key,
            'secret': api_secret,
            'options': {
                'defaultType': 'future',
            }})
        self.asset = f'{self.base}/{self.quote}'
        balance = self.exchange.fetch_balance()
        self._total_cash = balance[self.quote]['total']

    def on_trading_iteration(self):
        self._logger.info(f'Start trading iteration: exchange=Binance, symbol={self.base}/{self.quote}, interval={self.interval}, leverage={self.leverage}')
        self.exchange.set_leverage(self.leverage, self.asset)
        self.exchange.set_margin_mode(self.margin_mode, self.asset)
        balance = self.exchange.fetch_balance()
        open_position = self._get_position(self.base, balance['info']['positions'])
        ohlcv_df = self._fetch_ohlcv(self.asset, self.interval)
        sma_200 = ohlcv_df.ta.sma(length=200)
        bbands = ohlcv_df.ta.bbands(length=20, std=2).iloc[-1]
        atr = ohlcv_df.ta.atr(length=14).iloc[-1]
        volume_threshold = 3 * ohlcv_df['volume'].rolling(6 * 7).mean().iloc[-1] # 6 * 7 = (1 week in 4h timeframe)
        target_contract_num = self._total_cash * 0.01 / atr
        current_contract_num = 0
        if open_position != None:
            current_contract_num = open_position['contracts']
        if target_contract_num == 0:
            pyramiding_status = 0
        else:
            pyramiding_status = ceil(current_contract_num / target_contract_num)
        self._logger.info(f'close: {ohlcv_df["close"].iloc[-1]}, \
sma_200: {sma_200.iloc[-2]} {sma_200.iloc[-1]}, \
bbands_upper: {bbands["BBU_20_2.0"]}, \
bbands_mid: {bbands["BBM_20_2.0"]}, \
bbands_lower: {bbands["BBL_20_2.0"]}, \
1 tick before volume: {ohlcv_df["volume"].iloc[-2]}, \
volume_threshold: {volume_threshold}')
        
        # open long position
        if open_position == None or (open_position['side'] == 'long') \
            and pyramiding_status <= self.pyramiding \
            and ohlcv_df['close'].iloc[-1] > bbands['BBU_20_2.0'] \
            and sma_200.iloc[-1] > sma_200.iloc[-2] \
            and ohlcv_df['volume'].iloc[-2] > volume_threshold:
                order = self.exchange.create_market_buy_order(self.asset, target_contract_num)
                self._logger.info(f'Open long position; \
symbol:{self.asset}, \
price:{order["cost"]}, \
avg_price:{order["average"]}, \
qty: {target_contract_num}, \
fee:{order["fee"]["cost"]}{order["fee"]["currency"]}')

        # open short position
        if open_position == None or (open_position['side'] == 'short') \
            and pyramiding_status <= self.pyramiding \
            and ohlcv_df['close'].iloc[-1] < bbands['BBL_20_2.0'] \
            and sma_200.iloc[-1] < sma_200.iloc[-2] \
            and ohlcv_df['volume'].iloc[-2] > volume_threshold:
                order = self.exchange.create_market_sell_order(self.asset, target_contract_num)
                self._logger.info(
                    f'Open short position; \
symbol:{self.asset}, \
price:{order["cost"]}, \
avg_price:{order["average"]}, \
qty: {target_contract_num}, \
fee:{order["fee"]["cost"]}{order["fee"]["currency"]}')

        # close long position
        if (open_position != None and open_position['side'] == 'long') and ohlcv_df['close'].iloc[-1] <= bbands['BBM_20_2.0']:
            order = self.exchange.create_market_sell_order(self.asset, current_contract_num)
            self._logger.info(
                f'Close long position; \
symbol:{self.asset}, \
price:{order["cost"]}, \
avg_price:{order["average"]}, \
qty: {current_contract_num}, \
fee:{order["fee"]["cost"]}{order["fee"]["currency"]}, \
realized_pnl:{order["info"]["realizedPnl"]}')

        # close short position
        if (open_position != None and open_position['side'] == 'short') and ohlcv_df['close'].iloc[-1] >= bbands['BBM_20_2.0']:
            order = self.exchange.create_market_buy_order(self.asset, current_contract_num)
            self._logger.info(
                f'Close short position; \
symbol:{self.asset}, \
price:{order["cost"]}, \
avg_price:{order["average"]}, \
qty: {current_contract_num}, \
fee:{order["fee"]["cost"]}{order["fee"]["currency"]},\
realized_pnl:{order["info"]["realizedPnl"]}')
            
        self._logger.info(f'End trading iteration: exchange=Binance, symbol={self.base}/{self.quote}, interval={self.interval}, leverage={self.leverage}')

    def _fetch_ohlcv(self, asset, interval):
        ohlcv = self.exchange.fetch_ohlcv(asset, interval)
        df = pd.DataFrame(
            ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def _get_position(self, base, positions):
        for p in positions:
            if p['symbol'] == base:
                return p
        return None

if __name__ == '__main__':
    bb_breakout_BTC = BollingerbandBreakout('BTC', 'USDT', prio=2, interval='4h', pyramiding=2, leverage=1)

    schedule.every().day.at("01:00").do(bb_breakout_BTC.on_trading_iteration)
    schedule.every().day.at("05:00").do(bb_breakout_BTC.on_trading_iteration)
    schedule.every().day.at("09:00").do(bb_breakout_BTC.on_trading_iteration)
    schedule.every().day.at("13:00").do(bb_breakout_BTC.on_trading_iteration)
    schedule.every().day.at("17:00").do(bb_breakout_BTC.on_trading_iteration)
    schedule.every().day.at("21:00").do(bb_breakout_BTC.on_trading_iteration)

    while True:
        schedule.run_pending()
        time.sleep(1)