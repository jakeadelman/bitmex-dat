import requests
import json
from pprint import pprint
import sqlite3
import datetime, time
import psycopg2




class Bitmex(object):
        
        def __init__(self):
                self.now_h = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                self.five_mins_ago = datetime.datetime.utcnow()- datetime.timedelta(minutes=5)
                self.fma_h = self.five_mins_ago.strftime("%Y-%m-%d %H:%M")
                # print(now_h, fma_h)

                self.BPA = 0
                self.SPA = 0
                self.buys = 0
                self.sells = 0
                self.buycount = 0
                self.sellcount = 0
               


        def fetch_open_interest(self):
                STATS = "https://www.bitmex.com/api/v1" + '/stats'

                r = requests.get(url=STATS)

                data = r.json()
                for i in data:
                        if i['rootSymbol']=='XBT':
                                open_interest = i['openInterest']
                                return(open_interest)

        def fetch_longs_shorts(self):
                URL = "https://www.bitmex.com/api/v1" + "/trade"
                # print(self.now_h)
                filt = {}
                PARAMS ={
                        'symbol':'XBT',
                        'filter':json.dumps(filt),
                        'count':1000,
                        'startTime':self.fma_h,
                        'endTime':self.now_h
                        }

                r = requests.get(url=URL, params=PARAMS)
                # pprint(r.json())
                data = r.json()
              
                

                for dat in data:
                        if dat['side']=='Buy':
                                self.buys+=dat['size']
                                self.BPA+=dat['price']
                                self.buycount+=1
                        if dat['side']=='Sell':
                                self.sells+=dat['size']
                                self.SPA+=dat['price']
                                self.sellcount+=1
                print(self.buys, self.sells, self.BPA, self.SPA, len(data))
                try:
                        BPAfin=self.BPA//self.buycount
                        SPAfin=self.SPA//self.sellcount
                except:
                        BPAfin=data[-1]['price']
                        SPAfin=data[-1]['price']
                AVGprice=(self.BPA+self.SPA)/(self.buycount+self.sellcount)
                AVGprice= round(AVGprice,1)
                print(BPAfin, SPAfin,AVGprice)                
                long_short = {}
                long_short['longs'] = self.buys
                long_short['shorts'] = self.sells
                long_short['BPA'] = BPAfin
                long_short['SPA'] = SPAfin
                long_short['AVGprice']=AVGprice
                
                return(long_short)
                              
        def run(self):                       
                OI = self.fetch_open_interest()
                LS = self.fetch_longs_shorts()
                

                IQ = """INSERT INTO liquidity
                        (openinterest, long, short, bpa, spa, avgprice, timestamp)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s);"""

                VALS = (OI, LS['longs'], LS['shorts'], LS['BPA'], LS['SPA'], LS['AVGprice'], str(self.now_h))
                print(VALS)
                conn = psycopg2.connect(host='localhost',
                                        port="5432",
                                        database="bitmex", 
                                        user="jakeadelman", 
                                        password="jakeadelman")
                c = conn.cursor()
                c.execute(IQ, VALS)
                conn.commit()

# Bitmex().fetch_longs_shorts()
# Bitmex().run()
while True:
        if datetime.datetime.now().minute % 5 == 0:
                Bitmex().run()
        time.sleep(60)

#(market) close long + open short : market open long + close short˝