import pandas as pd

file_name1 = "./data/2024-05-01-upbit-BTC-book1.csv"
file_name2 = "./data/2024-05-01-upbit-BTC-book3.csv"
df1 = pd.read_csv(file_name1)
df2 = pd.read_csv(file_name1)

#DOING THIS BECAUSE DATA IS TOO BIG FOR GIT HUB
combined_df = pd.concat([df1, df2], ignore_index=True)
combined_df.to_csv("./data/2024-05-01-upbit-BTC-book.csv")