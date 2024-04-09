import time
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import logging

def save_to_csv(date, df: pd.DataFrame, logger):
    # 날짜&시간 가져와서 파일이름에 대입해서 저장하기
    file_name = f"book-{date}-bithumb-btc.csv"
    df.to_csv(f"./data/{file_name}")
    logger.info(f"saved file: {file_name}")

def get_book(logger) -> dict:
    book = {}
    response = requests.get('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
    # response = requests.get('https://api.bithumb.com/public/orderbook/ETH_KRW/?count=5')
    if response.status_code != 200:
        logger.error("BAD RESPONSE")
        raise Exception
    book = response.json()
    return book['data']

def main():
    #For logging
    logging.basicConfig(filename="./data/log/collect_orderbook.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    for _ in tqdm(range(17280)):
        data = get_book(logger)
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        date = now.strftime('%Y-%m-%d')

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
            continue
        
        df = pd.concat([bids, asks], ignore_index=True)
        #add timestamp
        timestamp = [timestamp] * 10
        df["timestamp"] = timestamp
        save_to_csv(date, df,logger)

        time.sleep(5)

if __name__ == "__main__":
    main()
