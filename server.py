import os
import http.server
import socketserver
import datetime
import requests
import json
from threading import Timer
from http import HTTPStatus
import pymongo
from pymongo import MongoClient
import csv

authorization_string = 'token 26ud7j6qh471oabu:xbU6v5zaQkUpojoAJIDd1b32zcS5Z1ew'

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        msg = 'BSE Research! you requested %s' % (self.path)
        self.wfile.write(msg.encode())


# ********PYMONGO*************
cluster = MongoClient("mongodb+srv://Cuesocial:RS8s6gJUc2FpnYK@cluster0.whjmu.mongodb.net/BSE-Insider?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE")

db=cluster["BSE-Insider"]
news_collection=db["news"]
reject_list_collection=db["reject_list"]
nse_list=db["nse_list"]
# ********PYMONGO*************


today = ''.join(str(datetime.date.today()).split('-'))
yesterday = ''.join(str(datetime.date.today()-datetime.timedelta(1)).split('-'))

                                       

def check_announcements(d):

    news_dict = []
   
    url = f'https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate={d}&strScrip=&strSearch=P&strToDate={d}&strType=C'
    print('requesting')
    resp = requests.get(url, headers={"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"})
    
    print('Recieved')
    
    if resp.status_code == 200:
        
        resp_dict = json.loads(resp.content.decode("UTF-8"))

        for doc in resp_dict['Table']:
            
            news_datetime_string = doc['DissemDT']
           

            if len(news_datetime_string.split('.'))==1:
                news_datetime_string+='.0'
            
            news_datetime = datetime.datetime.strptime(news_datetime_string,'%Y-%m-%dT%H:%M:%S.%f')
            
            market_open_time = datetime.datetime.strptime(news_datetime_string.split('T')[0]+' 9:15:00.00','%Y-%m-%d %H:%M:%S.%f')
            market_close_time = datetime.datetime.strptime(news_datetime_string.split('T')[0]+' 15:30:00.00','%Y-%m-%d %H:%M:%S.%f')
            print(doc['SLONGNAME'])
            if (market_open_time < news_datetime) and (market_close_time > news_datetime):

                url = f'https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w?quotetype=EQ&scripcode={doc["SCRIP_CD"]}'     
                resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"})
                ticker_doc = json.loads(resp.content.decode("UTF-8"))

                symbol = ticker_doc['SecurityId']
                exchange = 'BSE'

                # check market cap
                url = f'https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={doc["SCRIP_CD"]}'
                resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"})
               
                market_cap=''
                good_marketcap = False           
                print(json.loads(resp.content.decode("UTF-8"))['MktCapFull'],symbol)

                if  json.loads(resp.content.decode("UTF-8"))['MktCapFull'] != None:
                    if (',' in json.loads(resp.content.decode("UTF-8"))['MktCapFull']) or ('-' == json.loads(resp.content.decode("UTF-8"))['MktCapFull']):
                        good_marketcap = True
                        market_cap = json.loads(resp.content.decode("UTF-8"))['MktCapFull']
                    else:
                        market_cap = float(json.loads(resp.content.decode("UTF-8"))['MktCapFull'])
                        if market_cap > 200:
                            good_marketcap = True
                    
                    if good_marketcap:

                        # check if stock is on NSE
                        nse_ticker = nse_list.find_one({'ISIN_NUMBER':ticker_doc['ISIN']})

                        if nse_ticker:
                            exchange = 'NSE'
                            symbol = nse_ticker['SYMBOL']


                        news_time_string = (news_datetime_string.split('T')[1]).split('.')[0]
                        news_minute = news_time_string.split(':')[0] + ':' + news_time_string.split(':')[1] + ':' + '00'
                        # print('apply on',news_minute,news_datetime_string)

                        # find time after 5 minutes
                        from_time = f'{news_datetime_string.split("T")[0]}+{news_minute}'
                        from_time_datetime = datetime.datetime.strptime(from_time,'%Y-%m-%d+%H:%M:%S')
                        # print(from_time_datetime ,from_time_datetime + datetime.timedelta(minutes=5))

                        if (from_time_datetime < market_close_time - datetime.timedelta(minutes=30)):
                            to_time = datetime.datetime.strftime(from_time_datetime + datetime.timedelta(minutes=30),'%Y-%m-%d+%H:%M:%S')
                            with open('.csv') as csv_file:
                                csv_reader = csv.reader(csv_file, delimiter=',')
                            
                                for row in csv_reader:
                                    
                                    if (row[2]==symbol and row[11]==exchange):
                                        print(f'\t tradingsymbol:{row[2]} exchange: {row[11]} ')
                                        url = f'https://api.kite.trade/instruments/historical/{row[0]}/minute?from={from_time}&to={to_time}'
                                        
                                        resp = requests.get(url, headers={'X-Kite-Version': '3','Authorization':authorization_string})
                                        parsed_response = json.loads(resp.content.decode("UTF-8"))

                                        candles = parsed_response['data']['candles']
                                    
                                    
                                    

                                        if len(candles)>=30:                                      

                                            # Subject , Headline and attachment
                                            subject = doc['NEWSSUB']
                                            headline = doc['HEADLINE']
                                            attatchment = f'https://www.bseindia.com/xml-data/corpfiling/AttachLive/{doc["ATTACHMENTNAME"]}'
                                           
                                            # Promoter Holding 

                                            # percentage change in 2 mins
                                            percent_change2 = ((candles[1][4] - candles[0][1])/candles[0][1])*100

                                            # percentage change in 5 mins
                                            percent_change5 = ((candles[4][4] - candles[0][1])/candles[0][1])*100

                                            # percentage change in 10 mins
                                            percent_change10 = ((candles[9][4] - candles[0][1])/candles[0][1])*100
                    
                                            # percentage change in 15 mins
                                            percent_change15 = ((candles[14][4] - candles[0][1])/candles[0][1])*100

                                            # percentage change in 30 mins
                                            percent_change30 = ((candles[29][4] - candles[0][1])/candles[0][1])*100

                                            with open('result.csv', 'a') as csvfile: 
                                                # creating a csv writer object 
                                                csvwriter = csv.writer(csvfile) 
                                                    
                                                # writing the fields 
                                                csvwriter.writerow([symbol,exchange,news_datetime_string,subject,headline,attatchment,percent_change2,percent_change5,percent_change10,percent_change15,percent_change30,market_cap,]) 
                                                    
                                            break
                

           
    return 

dates = ["20210701","20210702","20210705","20210706","20210707","20210708","20210709","20210712","20210713","20210714","20210715","20210716","20210719","20210720","20210722","20210723","20210726","20210727","20210728","20210729","20210730"]

for d in dates:
    check_announcements(d) 

check_announcements() 

# *******SERVER******
port = int(os.getenv('PORT', 80))
print('Listening on port %s' % (port))
httpd = socketserver.TCPServer(('', port), Handler)
httpd.serve_forever()
# *******SERVER******

