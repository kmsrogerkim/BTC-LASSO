import pandas as pd

def main():
    df = pd.read_csv("./ai-crypto-project-3-live-btc-krw.csv")
    buy_df = df.loc[df['side'] == 0]
    sell_df = df.loc[df['side'] == 1]

    buy = buy_df['price'] * buy_df['quantity']
    sell = sell_df['price'] * sell_df['quantity']
    PnL = sell.sum()- buy.sum() - df['fee'].sum()
    remaining_btc =  buy_df['quantity'].sum() - sell_df['quantity'].sum()
    print(f"PnL: {PnL}\nRemaining BTC: {remaining_btc}")

if __name__ == "__main__":
    main()