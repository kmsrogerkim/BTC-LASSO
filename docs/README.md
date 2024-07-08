# Bitcoin Orderbook Collection and Implemention of Lasso Regression on Feature Data

## Notice
This repo was created as a student project for the "AI와 암호화폐이야기" lecture in HYU.

## 💬 Description
This directory does the following things:
1. Gather & organize the orderbook from the _bithumb_ exchange.
2. calculate essential & meaningful ***feature data*** for them, such as
    - mid price, market mid price, weighted mid price
    - book imbalance
3. Implement Lasso regression model on these data and calculate PnL

## ⚙️ Modules/Packages Used
- `pandas`: python module for data manipulation
- `stringr`: string operations in R
- `glmnet`: for fitting regularized regression models, such as lasso and elastic net, across a wide range of model types, including linear, logistic, Poisson, and Cox regression

These are listed in `requirements.txt` in the `Docs` folder. Use the below command to install these dependencies.
- ```pip install -r requirements.txt```

## 🛐 How to run

- ```pip install -r requirements.txt```
- ```python combine_book.py```
- Then, just run the files you want to try out.

## 🤖 Contributors
- 김민승 <[Github](https://github.com/rogerkimjazzlover)>
- 장선우 <[Github](https://github.com/banbanmu-han)>
