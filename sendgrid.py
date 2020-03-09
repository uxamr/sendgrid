import creds
import requests
import urllib.parse
from pandas.io.json import json_normalize
import pandas as pd
import sys
from utils import Redshift, Initialize, fechas, Upload_Redshift, New_Columns
from datetime import datetime
import time

class sendgrid(Redshift) :
    def __init__(self) :
        super().__init__()
        self.headers = {'authorization': 'Bearer {}'.format(creds.sendgrid["token"])}
    def messages(self, start_date = None, end_date = None) :
        """
        Tabla "messages".
        start_date y end_date son fechas en formato YYYY-MM-DD. Si no se proporcionan, se hace una extraccion completa.
        """
        if start_date != None and end_date != None :
            #Extraccion por intervalo de fechas
            start = datetime( int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10])).isoformat()
            end = datetime( int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10])).isoformat()
            start = str(start) + "Z"
            end = str(end) + "Z"
            print(start, end)
            query = 'last_event_time between timestamp "%s" and timestamp "%s"' %(start, end)
            query = urllib.parse.quote(str(query))
            endpoint = "https://api.sendgrid.com/v3/messages?limit=100000&query=%s" % query
            response = requests.get(endpoint, headers= self.headers)
            dat = response.json()
            df = json_normalize(dat['messages'])
            print(len(df))
            Upload_Redshift(df = df, name_table = "messages", engine = self.engine)
        elif start_date == None and end_date == None :
            #Extraccion completa
            query = 'last_event_time between timestamp "{}" and timestamp "{}"'
            f = fechas(inicio = "2019-10-01", tipo = "months").fechas
            ind = 0
            for i in f :
                start = datetime( int(i[0][0:4]), int(i[0][5:7]), int(i[0][8:10])).isoformat()
                end = datetime( int(i[1][0:4]), int(i[1][5:7]), int(i[1][8:10])).isoformat()
                start = (str(start)) + "Z"
                end = (str(end)) + "Z"
                print(start, end)
                aux_q = query.format( start, end)
                aux_q = urllib.parse.quote(str(aux_q))
                endpoint = "https://api.sendgrid.com/v3/messages?limit=100000&query=%s" % aux_q
                response = requests.get(endpoint, headers= self.headers)
                #print("remaining: ", response.headers["X-RateLimit-Remaining"])
                #print(response.headers["X-RateLimit-Reset"])
                #if int(response.headers["X-RateLimit-Remaining"]) == 0 :
                #    time.sleep( int( str(response.headers["X-RateLimit-Reset"])[0:3] ) + 120)
                dat = response.json()
                df = json_normalize(dat['messages'])
                print(len(df))
                if df.empty == False :
                    print(len(df))
                    if ind == 0 :
                        Initialize(dat = df, name = "messages", engine = self.engine, pkey="last_event_time")
                    New_Columns(tabla = df, eng = self.engine, name = "messages")
                    Upload_Redshift(df = df, name_table = "messages", engine = self.engine)
                    ind = ind + 1
        else :
            print("Deben introducirse dos fechas")
            sys.exit(0)
        return df
    def stats(self, start_date = None, end_date = None) :
        """
        Tabla "stats".
        start_date y end_date son fechas en formato YYYY-MM-DD. Si no se proporcionan, se hace una extraccion completa.
        """
        if start_date != None and end_date != None :
            #Extraccion por intervalo de fechas
            endpoint = "https://api.sendgrid.com/v3/stats?start_date=%s&end_date=%s" % (start_date, end_date)
            response = requests.get(endpoint, headers= self.headers)
            dat = response.json() 
            temp = []
            temp_d = []
            for i in dat :
                temp_d.append(i['date'])
                stats = json_normalize(i['stats'][0]['metrics'])
                temp.append(stats)
            dates = pd.DataFrame({"date":temp_d})
            df_stats = pd.concat(temp, sort= False, ignore_index=True) 
            df_stats = pd.concat([dates,df_stats], axis = 1)
            Upload_Redshift(df = df_stats, name_table = "stats", engine = self.engine)
        elif start_date == None and end_date == None :
            #Extraccion completa
            f = fechas(inicio = "2018-12-01", tipo = "months").fechas
            ind = 0
            for i in f :
                print(i[0], i[1])
                endpoint = "https://api.sendgrid.com/v3/stats?start_date=%s&end_date=%s" % (i[0], i[1])
                response = requests.get(endpoint, headers= self.headers)
                print("remaining: ", response.headers["X-RateLimit-Remaining"])
                dat = response.json()
                print(len(dat))
                temp = []
                temp_d = []
                for i in dat :
                    temp_d.append(i['date'])
                    stats = json_normalize(i['stats'][0]['metrics'])
                    temp.append(stats)
                dates = pd.DataFrame({"date":temp_d})
                df_stats = pd.concat(temp, sort= False, ignore_index=True) 
                df_stats = pd.concat([dates,df_stats], axis = 1)
                if ind == 0 :
                    Initialize(dat = df_stats, name = "stats", engine = self.engine, pkey="date")
                New_Columns(tabla = df_stats, eng = self.engine, name = "stats")
                Upload_Redshift(df = df_stats, name_table = "stats", engine = self.engine)
                ind = ind + 1
        else :
            print("Deben introducirse dos fechas")
            sys.exit(0)

        return df_stats

    def geo_stats(self, start_date = None, end_date = None) :
        """
        Tabla "geo_stats".
        start_date y end_date son fechas en formato YYYY-MM-DD. Si no se proporcionan, se hace una extraccion completa.
        """
        if start_date != None and end_date != None :
            #Extraccion por intervalo de fechas
            endpoint = "https://api.sendgrid.com/v3/geo/stats?start_date=%s&end_date=%s" % (start_date, end_date)
            response = requests.get(endpoint, headers= self.headers)
            dat = response.json() 
            dates = []
            types = []
            names = []
            metrics = []
            for i in dat :
                for j in i['stats'] :
                    dates.append(i['date'])
                    types.append(j['type'])
                    names.append(j['name'])
                    temp = json_normalize(j['metrics'])
                    metrics.append(temp)    
            df_temp = pd.DataFrame({"date":dates,"type":types,"name":names}) 
            df_gstats = pd.concat(metrics, sort=False, ignore_index=True)       
            df_gstats = pd.concat([df_temp, df_gstats], axis = 1)
            Upload_Redshift(df = df_gstats, name_table = "geo_stats", engine = self.engine)

        elif start_date == None and end_date == None :
            #Extraccion completa
            f = fechas(inicio = "2018-12-01", tipo = "months").fechas
            ind = 0
            for i in f :
                print(i[0], i[1])
                endpoint = "https://api.sendgrid.com/v3/geo/stats?start_date=%s&end_date=%s" % (i[0], i[1])
                response = requests.get(endpoint, headers= self.headers)
                print("remaining: ", response.headers["X-RateLimit-Remaining"])
                dat = response.json()
                dates = []
                types = []
                names = []
                metrics = []
                for i in dat :
                    for j in i['stats'] :
                        dates.append(i['date'])
                        types.append(j['type'])
                        names.append(j['name'])
                        temp = json_normalize(j['metrics'])
                        metrics.append(temp)    
                df_gstats = pd.concat(metrics, sort=False, ignore_index=True)
                df_temp = pd.DataFrame({"date":dates,"type":types,"name":names})    
                df_gstats = pd.concat([df_temp, df_gstats], axis = 1)
                if ind == 0 :
                    Initialize(dat = df_gstats, name = "geo_stats", engine = self.engine, pkey="date")
                New_Columns(tabla = df_gstats, eng = self.engine, name = "geo_stats")
                Upload_Redshift(df = df_gstats, name_table = "geo_stats", engine = self.engine)
                ind = ind + 1
        else :
            print("Deben introducirse dos fechas")
            sys.exit(0)

        return df_gstats
