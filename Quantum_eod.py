import pandas as pd
import shutil
import os
from datetime import date
from itertools import cycle
import numpy as np

pd.set_option('display.max_columns',50)

 
file_source = r'S:/DataMentah/'
file_destination = r'S:/'
 
get_files = os.listdir(file_source)
for g in get_files:
    if g.startswith("Rekap"):      
        Sukuk = pd.read_excel(file_source + g, index_col=None, sheet_name = 'Rekap', nrows= 31, usecols = 'A:F')
        Sukuk['Date'] = pd.to_datetime(str(date.today()))
        Sukuk['Date'] = pd.to_datetime(Sukuk['Date']).dt.strftime('%Y%d%m')
        Sukuk['TODAY FAIR PRICE1'] = Sukuk.apply(lambda x: "{:,.2f}".format(x['TODAY FAIR PRICE']), axis=1)
        list_drop = ['DATE', 'TODAY FAIR PRICE']
        Sukuk.drop(list_drop, axis=1, inplace=True)
        Sukuk.dropna(axis=0, how = 'any')
        Sukuk.rename(columns = {'Date': 'DATE','TODAY FAIR PRICE1': 'TODAY FAIR PRICE'},inplace=True)
        Sukuk = Sukuk[["DATE","SERIES","ISIN","NAME","TODAY FAIR PRICE","CURRENCY"]]
        
    if g.startswith("Kurs"):
        Fwd = pd.read_excel(file_source + g, skiprows = 3,  nrows= 56, usecols = 'P:R')        
        Fwd = Fwd[["BID","ASK"]]
        Fwd.loc[34,'BID'] = 0 #IDR EOD
        Fwd.loc[34,'ASK'] = 0 #IDR EOD
        Fwd['DATE'] = pd.to_datetime(str(date.today()))
        Fwd['DATE'] = pd.to_datetime(Fwd['DATE']).dt.strftime('%m/%d/%Y')
        Fwd['BID'] = Fwd.apply(lambda x: "{:,.2f}".format(x['BID']), axis=1)
        Fwd['ASK'] = Fwd.apply(lambda x: "{:,.2f}".format(x['ASK']), axis=1)        
        TenQuote = cycle(['ON','SW','2W','1M','2M','3M','6M','12M'])
        Fwd['Tenor Quote'] = [next(TenQuote) for Fwd in range(len(Fwd))]
        Tenor = cycle(['Today','1wk','2wk','1 Month','2 Months','3 Months','6 Months','12 Months'])
        Fwd['TENOR'] = [next(Tenor) for Fwd in range(len(Fwd))]            
        Curs = ['IDR','MYR','EUR','SGD','SAR','AUD','JPY']
        Fwd['CURNCY'] = np.repeat(Curs,8)
        Fwd['REUTERS QUOTE'] = Fwd['CURNCY'] + Fwd['Tenor Quote'] + '='
        Fwd = Fwd[["CURNCY","TENOR","REUTERS QUOTE","Tenor Quote","BID","ASK","DATE"]]
# -------------------------------------------------------------------------------------------------------------#
        Spot = pd.read_excel(file_source + g, index_col=None, skiprows = 52,  nrows= 5, usecols = 'F:G')
        Kurs_cols = ['BID', 'ASK']
        Spot.columns = Kurs_cols
        Spot_Jpy = pd.read_excel(file_source + g, index_col=None, skiprows = 57,  nrows= 1, usecols = 'F:G')
        Spot_Jpy.columns = Kurs_cols
        Spot_Aud = pd.read_excel(file_source + g, index_col=None, skiprows = 58,  nrows= 1, usecols = 'F:G')
        Spot_Aud.columns = Kurs_cols
        Spot = pd.concat([Spot,Spot_Aud,Spot_Jpy],axis=0, ignore_index=True, sort=False)
        Spot.rename(index={0: 7,1: 8,2: 9,3: 10,4: 11,5: 12,6: 13},inplace=True)
        Spot.loc[0] = Spot.loc[7,:] #IDR EOD
        Spot.loc[1] = round((Spot.loc[8,:] / Spot.loc[7,:]),9) #EUR EOD 
        Spot.loc[2] = round((Spot.loc[7,:] / Spot.loc[9,:]),9) #SAR EOD
        Spot.loc[3] = round((Spot.loc[7,:] / Spot.loc[10,:]),9) #MYR EOD
        Spot.loc[4] = round((Spot.loc[7,:] / Spot.loc[11,:]),9) #SGD EOD
        Spot.loc[5] = round((Spot.loc[12,:] / Spot.loc[7,:]),9) #AUD EOD 
        Spot.loc[6] = round((Spot.loc[7,:] / Spot.loc[13,:]),9) #JPY EOD
        Spot.sort_index(inplace=True)
        Spot1 = Spot.loc[[2,3,4,6], :]
        Spot1_cols = ['ASK','BID']
        Spot1.columns = Spot1_cols
        Spot = Spot.drop([2,3,4,6])
        Spot = pd.concat([Spot,Spot1],axis=0, ignore_index=False, sort=True)
        Spot.sort_index(inplace=True)
        Kurs = cycle(['IDR','EUR','SAR','MYR','SGD','AUD','JPY'])
        Spot['Currency'] = [next(Kurs) for Spot in range(len(Spot))]
        Spot['Date'] = pd.to_datetime(str(date.today()))
        Spot['Date'] = pd.to_datetime(Spot['Date']).dt.strftime('%m/%d/%Y')
        Rate = ['End of Day','Kurs BI']
        Spot['RateType'] = np.repeat(Rate,7)
        Spot['Bloomberg code'] = Spot['Currency'] + ' curncy'
        Spot['BID'] = Spot.apply(lambda x: "{:,.9f}".format(x['BID']), axis=1)
        Spot['ASK'] = Spot.apply(lambda x: "{:,.9f}".format(x['ASK']), axis=1)
        Spot['Bid_str'] = Spot.BID.astype(str)
        Spot['Ask_str'] = Spot.ASK.astype(str)
        Spot["Bid_str"]=Spot["Bid_str"].str.replace(',','')
        Spot["Ask_str"]=Spot["Ask_str"].str.replace(',','')
        Spot = Spot[["Currency","Bloomberg code","Bid_str","Ask_str","Date","RateType"]]
        Spot_cols = [" Currency ","Bloomberg code","Bid","Ask","Date","RateType"]
        Spot.columns = Spot_cols

print("Ukuran data Sukuk: %d baris, %d kolom." % Sukuk.shape)        
print(Sukuk)
Sukuk.to_csv(file_destination + 'GlobalBond_GBx.csv', sep=',',index=False)
print(' ---- ')
print("Ukuran data Forward: %d baris, %d kolom." % Fwd.shape)        
print(Fwd)
Fwd.to_csv(file_destination + 'FX FORWARDx.csv', sep=',',index=False)
print(' ---- ')
print("Ukuran data Spot: %d baris, %d kolom." % Spot.shape)        
print(Spot)
Spot.to_csv(file_destination + 'FX SPOTx.csv', sep=',',index=False)

print("GlobalBond_GBx.csv, FX SPOTx.csv, & FORWARDx.csv has been succesfully saved in bmi2(\\10.55.108.216)")

arr = os.listdir("S:/DataMentah")

import smtplib

sender_email = "sendi.putro@bankmuamalat.co.id"
rec_email = "sendi.putro@bankmuamalat.co.id","ardina.nidya@bankmuamalat.co.id","satyarsa.wienuri@bankmuamalat.co.id" ,"m.lutfi@bankmuamalat.co.id"
# rec_email = "sendi.putro@bankmuamalat.co.id"
passw = "Muamalat24"

with smtplib.SMTP('smtp.office365.com', 587) as smtp:
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login('20150503@bankmuamalat.co.id', passw)

    subject = 'Quantum Data Placement Report'
    body = 'Assalamualaikum wr wb\n\nPlease Check,\nSpot, Forward and Global_Bonds has been succesfully created using\n ' + str(arr) + ' data' + '\n' + ' and saved in bmi2(\\10.55.108.216)\n\nWasalam' + '\n\nThis email send by Task Scheduler'
    # body = 'Assalamualaikum wr wb\nPlease Check\nSpot, Forward and Global_Bonds has been succesfully created using\n ' + str(arr) + ' data' + '\n' + ' and saved in ' + file_destination +'\n'+ '\nWasalam'
    # ps = print('Sukuk')
    sk = Sukuk.head()
    sp = Spot.head()
    fw = Fwd.head()
    msg = f'Subject: {subject},\n\n{body},\n\nGlobalBond_GBx :\n\n{sk},\n\nFX SPOTx :\n\n{sp},\n\nFX FORWARDx :\n\n{fw}'
    smtp.sendmail(sender_email, rec_email, msg)
    print("Email has been sent to", rec_email)
