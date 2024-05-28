'''
This file was made to seperate the large 2024-05-01-upbit-BTC-book.csv
into two files. Our main code uses the 2024-05-01-upbit-BTC-book.csv, but it is too big
to upload to github. So this code is only for uploading to github.
'''
import pandas as pd

file_name1 = "./data/2024-05-01-upbit-BTC-book.csv"
df1 = pd.read_csv(file_name1)

#DOING THIS BECAUSE DATA IS TOO BIG FOR GIT HUB
df1_1 = df1.iloc[:1300000 ]
df1_2 = df1.iloc[1300000:]
df1_1.to_csv("./data/2024-05-01-upbit-BTC-book1.csv")
df1_2.to_csv("./data/2024-05-01-upbit-BTC-book2.csv")