import time
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import logging

from multiprocessing import Process

def save_to_csv(date, ord_currency, df: pd.DataFrame, logger):
    '''
    ord_currency = btc or eth
    '''
    file_name = f"book-{date}-bithumb-{ord_currency}.csv"
    old_df = pd.read_csv(f"./data/{file_name}")
    df = pd.concat([old_df, df], ignore_index=True)
    df.to_csv(f"./data/{file_name}", index=False)
    logger.info(f"saved file: {file_name}")

def get_book(ord_currency: str, logger) -> dict:
    '''
    ord_currency = BTC or ETH
    '''
    book = {}
    response = requests.get(f"https://api.bithumb.com/public/orderbook/{ord_currency}_KRW/?count=5")
    if response.status_code != 200:
        logger.error("BAD RESPONSE")
        raise Exception
    book = response.json()
    return book['data']

def main_task(ord_currency, logger, i):
    data = get_book(ord_currency, logger)
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    date = now.strftime('%Y-%m-%d')
    if i == 0:
        #Initialize DF
        empty_df = pd.DataFrame()
        empty_df.to_csv(f"./data/book-{date}-bithumb-{ord_currency.lower()}.csv")
    #bids를 pandas의 데이터프레임으로 바꾸고 정렬
    try:
        bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric)
        bids.sort_values('price', ascending=False, inplace=True)
        bids = bids.reset_index(); del bids['index']
        bids['type'] = 0
    except Exception as e:
        logger.error(f"{e}")

    #ask를 pandas의 데이터프레임으로 바꾸고 정렬
    try:
        asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric)
        asks.sort_values('price', ascending=True, inplace=True)
        asks['type'] = 1
    except Exception as e:
        logger.error(f"{e}")
        time.sleep(5)
        return False
    
    df = pd.concat([bids, asks], ignore_index=True)
    timestamp = [timestamp] * 10
    df["timestamp"] = timestamp
    ord_currency = ord_currency.lower()
    save_to_csv(date, ord_currency, df,logger)

def main():
    #For logging
    logging.basicConfig(filename="./data/log/collect_orderbook.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    for i in tqdm(range(17280)):
        start_time = time.time()

        p1 = Process(target=main_task, args=("ETH", logger, i))
        p2 = Process(target=main_task, args=("BTC", logger, i))

        p1.start()
        p2.start()

        p1.join() 
        p2.join() 

        remaining = time.time() - start_time
        if remaining < 5:
            time.sleep(5 - remaining)

if __name__ == "__main__":
    main()
