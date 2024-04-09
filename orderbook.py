import time
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import logging

def ReAttemptUntilFailure(max_attempt: int):
    '''
    An decorator function
    Waits for 30 seconds when the function fails.
    '''
    def ReAttemptUntilFailureFunction(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts != max_attempt:
                if attempts == max_attempt:
                    raise Exception
                try:
                    data = func(*args, **kwargs)
                except Exception:
                    attempts += 1
                    time.sleep(10)
                    continue
                break
            return data
        return wrapper
    return ReAttemptUntilFailureFunction

def save_to_csv(df: pd.DataFrame, logger):
    # 날짜&시간 가져와서 파일이름에 대입해서 저장하기
    timestamp = datetime.now()
    req_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    # print(req_timestamp, type(req_timestamp))
    file_name = req_timestamp + "-bithumb-BTC-orderbook.csv"
    df.to_csv(f"./data/{file_name}")
    logger.info(f"saved file: {file_name}")

@ReAttemptUntilFailure(max_attempt=5)
def get_book(logger) -> dict:
    book = {}
    response = requests.get('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=10')
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

        # df = bids.append(asks) #업데이트 필요
        df = pd.concat([bids, asks], ignore_index=True) #업데이트 된 버전
        save_to_csv(df,logger)

        time.sleep(5)
    return 0

if __name__ == "__main__":
    main()
