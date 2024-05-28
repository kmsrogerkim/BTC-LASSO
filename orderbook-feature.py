import pandas as pd
import math
from tqdm import tqdm

#bid = 0, ask = 1

class Feature():
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.max_itr = int(len(df)/30)

    def get_book_delta(self, first: bool, diff: tuple, var: dict, cur: dict):
        ratio, level, interval = 0.2, 5, 1

        decay = math.exp(-1.0/interval)
        
        curBidQty = cur['bid_qty']
        curAskQty = cur['ask_qty']
        curBidTop =  cur['top_bid']
        curAskTop = cur['top_ask']

        if first:
            var['prevBidQty'] = curBidQty
            var['prevAskQty'] = curAskQty
            var['prevBidTop'] = curBidTop
            var['prevAskTop'] = curAskTop
            return 0.0
        
        prevBidQty = var['prevBidQty']
        prevAskQty = var['prevAskQty']
        prevBidTop = var['prevBidTop']
        prevAskTop = var['prevAskTop']
        bidSideAdd = var['bidSideAdd']
        bidSideDelete = var['bidSideDelete']
        askSideAdd = var['askSideAdd']
        askSideDelete = var['askSideDelete']
        bidSideTrade = var['bidSideTrade']
        askSideTrade = var['askSideTrade']
        bidSideFlip = var['bidSideFlip']
        askSideFlip = var['askSideFlip']
        bidSideCount = var['bidSideCount']
        askSideCount = var['askSideCount'] 
            
        if curBidQty > prevBidQty:
            bidSideAdd += 1
            bidSideCount += 1
        if curBidQty < prevBidQty:
            bidSideDelete += 1
            bidSideCount += 1
        if curAskQty > prevAskQty:
            askSideAdd += 1
            askSideCount += 1
        if curAskQty < prevAskQty:
            askSideDelete += 1
            askSideCount += 1
        if curBidTop < prevBidTop:
            bidSideFlip += 1
            bidSideCount += 1
        if curAskTop > prevAskTop:
            askSideFlip += 1
            askSideCount += 1

        (count1, count0, units_traded1, units_traded0, price1, price0) = diff
        
        bidSideTrade += count1
        bidSideCount += count1
        
        askSideTrade += count0
        askSideCount += count0
        
        if bidSideCount == 0:
            bidSideCount = 1
        if askSideCount == 0:
            askSideCount = 1

        bidBookV = (-bidSideDelete + bidSideAdd - bidSideFlip) / (bidSideCount**ratio)
        askBookV = (askSideDelete - askSideAdd + askSideFlip ) / (askSideCount**ratio)
        tradeV = (askSideTrade/askSideCount**ratio) - (bidSideTrade / bidSideCount**ratio)
        bookDIndicator = askBookV + bidBookV + tradeV
        
        var['bidSideCount'] = bidSideCount * decay
        var['askSideCount'] = askSideCount * decay
        var['bidSideAdd'] = bidSideAdd * decay
        var['bidSideDelete'] = bidSideDelete * decay
        var['askSideAdd'] = askSideAdd * decay
        var['askSideDelete'] = askSideDelete * decay
        var['bidSideTrade'] = bidSideTrade * decay
        var['askSideTrade'] = askSideTrade * decay
        var['bidSideFlip'] = bidSideFlip * decay
        var['askSideFlip'] = askSideFlip * decay

        var['prevBidQty'] = curBidQty
        var['prevAskQty'] = curAskQty
        var['prevBidTop'] = curBidTop
        var['prevAskTop'] = curAskTop
    
        return bookDIndicator

    def get_diff_count_units (self, diff):
        count1 = count0 = units_traded1 = units_traded0 = 0
        price1 = price0 = 0

        diff_len = len (diff)
        if diff_len == 1:
            row = diff.iloc[0]
            if row['type'] == 1:
                count1 = row['count']
                units_traded1 = row['units_traded']
                price1 = row['price']
            else:
                count0 = row['count']
                units_traded0 = row['units_traded']
                price0 = row['price']

            return (count1, count0, units_traded1, units_traded0, price1, price0)

        elif diff_len == 2:
            row_1 = diff.iloc[1]
            row_0 = diff.iloc[0]
            count1 = row_1['count']
            count0 = row_0['count']

            units_traded1 = row_1['units_traded']
            units_traded0 = row_0['units_traded']
            
            price1 = row_1['price']
            price0 = row_0['price']

            return (count1, count0, units_traded1, units_traded0, price1, price0)

    def get_ten_row(self):
        """gets the top 10 rows, then delete it from original"""
        # ans = self.df.iloc[:10]
        ans = self.df.iloc[:30]
        # self.df = self.df.drop(self.df.index[:10])
        self.df = self.df.drop(self.df.index[:30])

        #only get top 5 rows
        ans = ans.drop(ans.index[5:15])
        ans = ans.drop(ans.index[10:20])

        ans = ans.reset_index(drop=True)
        return ans

    def get_quantity(self, df) -> list:
        """
        Returns:
            [bid_qty, ask_qty]
        """
        bid_qty, ask_qty = 0
        for i in range(5):
            bid_qty += df.iloc[i]
            ask_qty += df.iloc[5+i]
        return [bid_qty, ask_qty]

    def get_mid_price(self, df) -> dict:
        """
        Args:
            df(pd.DataFrame): a 10 row dataframe

        Returns:
            response(dict):
                - top_bid
                - top_aks
                - mid_price
        """
        price = df.loc[:, "price"]
        response = {
            "top_bid":price.iloc[0], 
            "top_ask":price.iloc[5],
            "mid_price":0.0
        }
        response["mid_price"] = (price[0]+price[5])/2
        return response

    def get_other_mid_prices(self, df: pd.DataFrame) -> dict:
        res = {
            "wt":0,
            "mkt":0,
        }
        price = df.loc[:, "price"]
        quantity = df.loc[:, "quantity"]
        bid_mean = price.iloc[2]
        ask_mean = price.iloc[7]
        res['wt'] = (bid_mean + ask_mean) * 0.5

        bid_top_qty = quantity.iloc[0]
        ask_top_qty = quantity.iloc[5]
        a = price[0]*ask_top_qty
        b = price[5]*bid_top_qty
        c = bid_top_qty + ask_top_qty
        res['mkt'] = (a+b)/c
        return res

    def get_book_imbalance(self, df, mid_price: float) -> float:
        ratio = 0.2
        interval = 1

        df_quantity = df.loc[:, 'quantity']
        df_price = df.loc[:, 'price']
        ask_qty, bid_qty = 0, 0
        ask_px, bid_px = 0, 0

        for i in range(5):
            bid_qty += df_quantity.iloc[i] ** ratio
            ask_qty += df_quantity.iloc[5+i] ** ratio
            bid_px += df_price.iloc[i]* df_quantity.iloc[i] ** ratio
            ask_px += df_price.iloc[5+1] * df_quantity.iloc[5+i] ** ratio

        book_price = (((ask_qty*bid_px)/bid_qty) + ((bid_qty*ask_px)/ask_qty)) / (bid_qty+ask_qty)
        book_imbalance = (book_price - mid_price) / interval
        return book_imbalance

    def get_quantity(self, df):
        ask = df.loc[:5, "quantity"]
        bid = df.loc[5:, "quantity"]
        return [ask.sum(), bid.sum()]

    def get_timestamp(self, df) -> str:
        return str(df['timestamp'].iloc[0])

def save_to_csv(date, ord_currency, df: pd.DataFrame):
    """ord_currency(str): btc or eth"""
    file_name = f"{date}-bithumb-{ord_currency}-feature.csv"
    df.to_csv(f"./data/{file_name}", index=False)

def read_csv(file_name: str):
    df = pd.read_csv(file_name)
    date = file_name[7:17]
    ord_currency = file_name[24:27]
    return df, date, ord_currency

def get_feature_csv(df, trade_df):
    feature = Feature(df)

    feat_df = {
        "book-delta-v1-0.2-5-1":[],
        "book-imbalance-0.2-5-1":[],
        "mid_price":[],
        "mid_price_wt":[],
        "mid_price_mkt":[],
        "timestamp":[]
    }

    groups = list(trade_df.groupby('timestamp'))

    first = True
    var = {
        'prevBidQty' : 0,
        'prevAskQty' : 0,
        'prevBidTop' : 0,
        'prevAskTop' : 0,
        'bidSideAdd' : 0,
        'bidSideDelete' : 0,
        'askSideAdd' : 0,
        'askSideDelete' : 0,
        'bidSideTrade' : 0,
        'askSideTrade' : 0,
        'bidSideFlip' : 0,
        'askSideFlip' : 0,
        'bidSideCount' : 0,
        'askSideCount' : 0,
    }
    for i in tqdm(range(feature.max_itr)):
        ten_row = feature.get_ten_row()

        cur = feature.get_mid_price(ten_row)
        cur["bid_qty"] = feature.get_quantity(df)[0]
        cur["ask_qty"] = feature.get_quantity(df)[1]

        feat_df['mid_price_wt'].append(feature.get_other_mid_prices(ten_row)['wt'])
        feat_df['mid_price_mkt'].append(feature.get_other_mid_prices(ten_row)['mkt'])

        feat_df["mid_price"].append(cur["mid_price"])
        feat_df["timestamp"].append(feature.get_timestamp(ten_row))
        feat_df['book-imbalance-0.2-5-1'].append(feature.get_book_imbalance(ten_row, cur['mid_price']))

        trade_grp = groups[i][1]
        diff = feature.get_diff_count_units(trade_grp)

        if first:
            book_imbalance = feature.get_book_delta(first=True, diff=diff, var=var, cur=cur)
            feat_df['book-delta-v1-0.2-5-1'].append(0.0)
            first = False
        else:
            book_imbalance = feature.get_book_delta(first=False, diff=diff, var=var, cur=cur)
            feat_df['book-delta-v1-0.2-5-1'].append(book_imbalance)
    return feat_df

def main():
    file_name1 = "./data/2024-05-01-upbit-BTC-book.csv"
    file_name2 = "./data/2024-05-01-upbit-BTC-trade.csv"
    df1, date, ord_currency1 = read_csv(file_name1)
    trade_df, date, ord_currency2 = read_csv(file_name2)
    # df1, date, ord_currency1 = read_csv("./data/book-2018-07-22-bithumb-btc.csv")

    feat_dict = get_feature_csv(df1, trade_df)
    feat_df = pd.DataFrame(feat_dict)

    save_to_csv(date, ord_currency1, feat_df)

    return 0

if __name__=="__main__":
    main()