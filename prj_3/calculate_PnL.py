import pandas as pd

class Trader():
    def __init__(self) -> None:
        self.btc = 0
        self.balance = 0
    
    def buy(self, price, quantity):
        self.balance -= price*quantity
        self.btc += quantity
    
    def sell(self, price, quantity):
        self.balance += price*quantity
        self.btc -= quantity
    
    def get_PnL(self, df):
        buy_df = df.loc[df['side'] == 0]
        sell_df = df.loc[df['side'] == 1]

        for i in range(len(buy_df)):
            price = buy_df.iloc[i]['price']
            quantity = buy_df.iloc[i]['quantity']
            self.buy(price, quantity)
        for i in range(len(buy_df)):
            price = sell_df.iloc[i]['price']
            quantity = sell_df.iloc[i]['quantity']
            self.sell(price, quantity)

def main():
    df = pd.read_csv("./ai-crypto-project-3-live-btc-krw.csv")
    trader = Trader()
    trader.get_PnL(df)
    print(trader.balance, trader.btc)

if __name__ == "__main__":
    main()