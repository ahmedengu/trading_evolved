import pandas as pd

data = pd.read_csv("spy.csv", index_col=0, parse_dates=[0])

data['pct'] = data['Close'].pct_change()

start = '2016/5/5'
end = '2017'


data_window = data[start:end]

min_pct = data_window['pct'].min()
max_pct = data_window['pct'].max()
std = data_window['pct'].std()

