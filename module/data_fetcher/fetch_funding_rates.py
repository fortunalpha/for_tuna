import asyncio
import aiohttp
# import ccxt.async_support as ccxt
import ccxt
import duckdb
import os
import pandas as pd
from datetime import timedelta, datetime as dt
from pprint import pprint
# from . import PROJECT_ROOT_PATH

EXCHANGE_NAME = 'binance'
# ccxt_params = {'options': {
#     'defaultType': 'future',
# }}
exchange = ccxt.binance()

# async def fetch_all_funding_rates():
#     start = dt(2019, 9, 10, 17, 0 ,0)
#     tasks = []
#     async with exchange as ex:
#         universe = await fetch_universe(ex)
#         async with aiohttp.ClientSession() as session:
#             for symbol in universe:
#                 tasks.append(fetch_funding_rate(session, ex, symbol, start, universe))
#             return await asyncio.gather(*tasks)

# async def fetch_universe(exchange):
#     universe = []
#     markets = await exchange.load_markets()
#     for symbol in markets:
#         quote = symbol.split('/')[-1]
#         if quote == 'USDT':
#             universe.append(symbol)
#     return universe
    
# async def fetch_funding_rate(session, exchange, symbol, start, universe):
#     funding_rates = []
#     since = start
#     limit = 1000
    
#     now = dt.now()
#     for i, symbol in enumerate(universe):
#         while since < now:
#             try:
#                 funding_rate_history = await exchange.fetch_funding_rate_history(symbol=symbol, since=int(since.timestamp() * 1000), limit=limit)
#                 funding_rates.extend([(symbol, dt.fromtimestamp(int(fr['info']['fundingTime']) / 1000), fr['info']['fundingRate']) for fr in funding_rate_history])
#                 since += timedelta(hours=limit * 8)
#             except Exception as e:
#                 break
#         print(f'{symbol} is finished. count: {i}', flush=True)
#         since = start
#     return funding_rates

def fetch_price(universe):
    start = dt(2019, 9, 10, 17, 0 ,0)
    since = start
    now = dt.now()
    limit = 500
    price_ohlcv = []
    for i, symbol in enumerate(universe):
        while since < now:
            try:
                ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe='4h', since=int(since.timestamp() * 1000), limit=limit)
                price_ohlcv.extend([{'symbol': symbol, 'timestamp': pi[0], 'open': pi[1], 'high': pi[2], 'low': pi[3], 'close': pi[4], 'volume': pi[5]} for pi in ohlcv])
                since += timedelta(hours=limit * 4)
            except Exception as e:
                print(f'{e}')
                break
        print(f'{symbol} is finished. count: {i}', flush=True)
        since = start
    return price_ohlcv


def fetch_premium_index(universe):
    start = dt(2019, 9, 10, 17, 0 ,0)
    since = start
    now = dt.now()
    limit = 1500
    premium_index = []
    for i, symbol in enumerate(universe):
        while since < now:
            try:
                premium_index_ohlcv = exchange.fetch_premium_index_ohlcv(symbol=symbol, timeframe='4h', since=int(since.timestamp() * 1000), limit=limit)
                premium_index.extend([{'symbol': symbol, 'timestamp': pi[0], 'open': pi[1], 'high': pi[2], 'low': pi[3], 'close': pi[4]} for pi in premium_index_ohlcv])
                since += timedelta(hours=limit * 4)
            except Exception as e:
                print(f'{e}')
                break
        print(f'{symbol} is finished. count: {i}', flush=True)
        since = start
    return premium_index

def fetch_funding_rate(session, exchange, start, universe):
    since = start
    limit = 1000
    funding_rates = []
    now = dt.now()
    for i, symbol in enumerate(universe):
        while since < now:
            try:
                funding_rate_history = exchange.fetch_funding_rate_history(symbol=symbol, since=int(since.timestamp() * 1000), limit=limit)
                funding_rates.extend([{'symbol': symbol, 'datetime': fr['datetime'], 'fundingRate': fr['info']['fundingRate'], 'markPrice': fr['info']['markPrice']} for fr in funding_rate_history])
                since += timedelta(hours=limit * 8)
            except Exception as e:
                break
        print(f'{symbol} is finished. count: {i}', flush=True)
        since = start
    return funding_rates

def fetch_universe():
    universe = []
    markets = exchange.load_markets()
    for symbol in markets:
        quote = symbol.split('/')[-1]
        if quote == 'USDT':
            universe.append(symbol)
    return universe

if __name__ == '__main__':
    universe=fetch_universe()
    result = fetch_price({'BTC/USDT'})
    df = pd.DataFrame(result)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    pprint(df)
    df.to_csv('btc_spot_price_ohlcv.csv')
    # universe = fetch_universe()
    # result = fetch_funding_rate(None, exchange, dt(2019, 9, 10, 17, 0 ,0), universe)
    # df = pd.DataFrame(result)
    # df.to_csv('funding_rates.csv')
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # db_name = f'{EXCHANGE_NAME}.db'
    # # db_path = os.path.join(PROJECT_ROOT_PATH, 'data', 'db', db_name)
    # con = duckdb.connect(f'{db_name}')
    # con.execute('''
    #     CREATE TABLE IF NOT EXISTS funding_rates (
    #         symbol TEXT,
    #         timestamp TIMESTAMP,
    #         funding_rate DOUBLE
    #     )
    # ''')
    # results = asyncio.run(fetch_all_funding_rates())
    # for funding_rates in results:
    #     if funding_rates:
    #         con.executemany("INSERT INTO funding_rates VALUES (?, ?, ?)", funding_rates)
    
    # df = con.execute("SELECT * FROM funding_rates WHERE symbol = 'ONDO/USDT' LIMIT 10").fetchdf()
    # print(df)
