import ccxt
import matplotlib.pyplot as plt
import pandas as pd
from datetime import date, timedelta
import time
import threading
import os 

class Pairs(object):

    def __init__(self):
        pass

    def get_historical_data(self, since, market, resolution):
        exchange = ccxt.ftx({'enableRateLimit': True})
        since = exchange.parse8601(f"{since}T00:00:00Z")
        params = {'market_name': market}  # https://github.com/ccxt/ccxt/wiki/Manual#overriding-unified-api-params
        limit = None
        # specify any existing symbol here ↓ (it does not matter, because it is overrided in params)
        ohlcv = exchange.fetch_ohlcv(market, resolution, since, limit, params)
        return ohlcv

    def download_historical_data(self,start_date, ticker, resolution):
        data = self.get_historical_data(start_date, ticker, resolution)
        file_name = f"data/{ticker}_{resolution}_{start_date}.csv"
        df = pd.DataFrame(data, columns=['time','open','high','low','close','volume'])
        df.to_csv(file_name)
        return file_name

    def read_historical_data(self,start_date, ticker, resolution):
        data = pd.read_csv(f"data/{ticker}_{resolution}_{start_date}.csv")
        return pd.DataFrame(data, columns=['time','open','high','low','close','volume'])

    def compare(date, t1, res="1h",long=False,tickers = ["BTC","ETH","LUNA", "SOL","AVAX","BNB","MATIC","NEAR","ATOM","ADA"]):
        sz = int(len(tickers) / 2)
        fig, axs = plt.subplots(2,sz,figsize=(20,15))
        
        info_list = list()
        if long:
            for i in range(len(tickers)):
                df = self.ls_index(date, t1, tickers[i]+"-PERP", res)
                long = t1.removesuffix("-PERP")
                axs[int(i/sz)][int(i%sz)].set_title(f"{long}/{tickers[i]}")
                axs[int(i/sz)][int(i%sz)].plot(df['time'], df['returns'])     
                max, min, cur = self.analyze(df,t1,tickers[i])
                info_list.append([f"{long}/{tickers[i]}",max,min,cur])
            
            plt.show()
        
                
        else:
            for i in range(len(tickers)):
                df = self.ls_index(date, tickers[i]+"-PERP",t1, res)
                long = t1.removesuffix("-PERP")
                axs[int(i/sz)][int(i%sz)].set_title(f"{tickers[i]}/{long}")
                axs[int(i/sz)][int(i%sz)].plot(df['time'], df['returns'])     
                max, min, cur = self.analyze(df,t1,tickers[i])
                info_list.append([f"{tickers[i]}/{long}",max,min,cur])
            
        info = pd.DataFrame(info_list,columns=['pair','max_drawdown','max_return','current_return'])
        return info

    def analyze(self,df,t1,t2):
        size = len(df) 
        # print(f"In {size} hours ({round(size/24)} days), {t1.removesuffix('-PERP')}/{t2} returned {round((df.iloc[size-1]['returns'] - 1) * 100,2)}%")
        maxdrawdown =  round((min(df['returns']) - 1 )*100,2)
        max_return =  round((max(df['returns'])-1) * 100,2)
        # print(f"The max drawdown was {maxdrawdown}% and the max return was {max_return}%")
        current_return = (df['returns'][size-1] - 1) * 100
        return maxdrawdown, max_return, current_return

    def ls_index(self,start_date, long, short, resolution):
        try:
            s = self.read_historical_data(start_date, short, resolution)
            l = self.read_historical_data(start_date, long, resolution)
        except FileNotFoundError as e:
            s_name = self.download_historical_data(start_date, short, resolution)
            l_name = self.download_historical_data(start_date, long, resolution)
            s = self.read_historical_data(start_date, short, resolution)
            l = self.read_historical_data(start_date, long, resolution)
        df = pd.DataFrame(columns=['time','l','s'])
        df['time'] = s['time']
        df['s'] = s['close']
        df['l'] = l['close']
        df['ls'] = df['l'] / df['s']
        df['returns'] = df['ls'] / df['ls'][0]
        
        return df

    def index(self, weights, lookback_window = 30, starting_balance = 1000):
        resolution = '1h'
        start_date = date.today() - timedelta(lookback_window)
        ohlc_data = dict()
        holding = dict()
        for ticker, weight in weights.items():
            try:
                ohlc = self.read_historical_data(start_date, f"{ticker}-PERP", resolution)
            except FileNotFoundError as e:
                ohlc_filename = self.download_historical_data(start_date, f"{ticker}-PERP", resolution)
                ohlc = self.read_historical_data(start_date, f"{ticker}-PERP", resolution)
            
            holding[ticker] = weight * starting_balance / ohlc['open'][0]
            ohlc['return'] = ohlc['close'] / ohlc['open'][0]
            if holding[ticker] < 0:
                ohlc['value'] = weight * starting_balance * (-1/ohlc['return'])
                ohlc['pnl'] = ohlc['value'] + weight*starting_balance

            else:
                ohlc['value'] = weight * starting_balance * ohlc['return']
                ohlc['pnl'] = ohlc['value'] - weight*starting_balance

            ohlc_data[ticker] = ohlc
        windowlength = len(list(ohlc_data.values())[0]) - 1
        va = [0]*(windowlength)
        plt.figure(figsize=(15,10))
        for k,v in ohlc_data.items():
            for i in range(len(va)):
                va[i] += v['pnl'][i]
            plt.plot(v['return'], label = k)
        pct_return = [(i/starting_balance) + 1 for i in va]
        plt.plot(pct_return, color='black', label='return')
        plt.legend()
        print(f"min drawdown: {100*(min(pct_return) - 1)}%")
        print(f"max return: {100*(max(pct_return) - 1)}%")
        print(f"current return: {100*(pct_return[-1] - 1)}%")
        for k,v in weights.items():
            print(f"{k} : {v}")

        return ohlc_data, va

class Index(object):

     def __init__(self, name, weights, amount):
         os.mkdir(name)
         self.name = name
         self.weights = weights
         self.tickers = [i for i in weights.keys()]
         self.amount = amount
         self.ftx = ccxt.ftx()
         
         thread = threading.Thread(target=self.fetch_prices, args=())
         thread.daemon = True                            # Daemonize thread
         thread.start() 
         
         self.data_to_csv()

     def data_to_csv(self):
         # todo: balance to csv
        with open(f"{self.name}/weights.csv", 'w') as f:
            for ticker, weight in self.weights.items():
                f.write("%s,%s\n"%(ticker, weight*self.amount))
            f.close()

     def fetch_prices(self):
        ftx = ccxt.ftx()
        exchange_id = 'ftx'
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'apiKey': '6rUlTDVKecEnNQv4t-yqv5PNvmMhdc1klR2wdPxq',
            'secret': 'ta-Y95hA99ZfRz0Pp93fG8Q6L6WP7qDAiGCulthl',
        })
        
        markets = ftx.load_markets()    
        
        while True:
            for ticker, weight in self.weights.items():
                # price = markets[f"{ticker}/USD:USD"]['info']['last']
                price = self.ftx.fetch_ticker(f"{ticker}-PERP")['ask']
                print(ticker, price)
                with open(f"{self.name}/{ticker}.csv", 'a') as f:
                    f.write("%s\n"%(price,))
                    f.close()
            time.sleep(60)



weights =  {'ETH' : 0.5 , 'SOL': 0.2, 'AVAX': 0.1, 'LUNA' :0.1, 'BTC' : 0.10}
d = Index("test6",weights, 100)

time.sleep(500)