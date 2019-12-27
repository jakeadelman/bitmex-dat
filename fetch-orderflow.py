import requests
import json
from pprint import pprint
import sqlite3
import datetime, time
import psycopg2




class Bitmex(object):

        now = datetime.datetime.utcnow()
        now_h = now.strftime("%Y-%m-%d %H:%M")
        five_mins_ago = now- datetime.timedelta(minutes=5)
        fma_h = five_mins_ago.strftime("%Y-%m-%d %H:%M")
        # print(now_h, fma_h)

        BPA = 0
        SPA = 0


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
                buys = 0
                sells = 0
                buycount = 0
                sellcount = 0
                

                for dat in data:
                        if dat['side']=='Buy':
                                buys+=dat['size']
                                self.BPA+=dat['price']
                                buycount+=1
                        if dat['side']=='Sell':
                                sells+=dat['size']
                                self.SPA+=dat['price']
                                sellcount+=1
                print(buys, sells, self.BPA, self.SPA, len(data))
                BPAfin=self.BPA//buycount
                SPAfin=self.SPA//sellcount
                AVGprice=(self.BPA+self.SPA)/(buycount+sellcount)
                AVGprice= round(AVGprice,1)
                print(BPAfin, SPAfin,AVGprice)                
                long_short = {}
                long_short['longs'] = buys
                long_short['shorts'] = sells
                long_short['BPA'] = BPAfin
                long_short['SPA'] = SPAfin
                long_short['AVGprice']=AVGprice
                
                return(long_short)
                              
        def run(self):                       
                OI = self.fetch_open_interest()
                LS = self.fetch_longs_shorts()
                n = self.now_h

                IQ = """INSERT INTO liquidity
                        (openinterest, long, short, bpa, spa, avgprice, timestamp)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s);"""

                VALS = (OI, LS['longs'], LS['shorts'], LS['BPA'], LS['SPA'], LS['AVGprice'], str(n))
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

#(market) close long + open short : market open long + close short