import os
from env import env
import requests
import urllib.parse
from datetime import date, datetime, timedelta
from pandas.io.json import json_normalize
import pandas as pd
from sqlalchemy import create_engine
from aws_utils import Initialize, New_Columns, Upload_S3, Copy_Redshift

class sendgrid(env) :
    
    def __init__(self) :
        super().__init__()
        self.headers = {'authorization': 'Bearer {}'.format(os.environ["SENDGRID_TOKEN"])}
        
    def tabla_mensajes(self, start, end) :
        start_date = (str(start)[:-7])
        end_date = (str(end))[:-7]
        
        start_date= (start_date.replace(" ","T")) + "Z"
        end_date = end_date.replace(" ", "T") + "Z"
        
        query = 'last_event_time between timestamp "%s" and timestamp "%s"' %(start_date, end_date)
        
        query = urllib.parse.quote(str(query))
        
        endpoint = "https://api.sendgrid.com/v3/messages?limit=1000&query=%s" % query
        
        response = requests.get(endpoint, headers= self.headers)
        dat = response.json()

        df = json_normalize(dat['messages'])
        
        return df
    
    def tabla_stats(self, start, end) :
        endpoint = "https://api.sendgrid.com/v3/stats?start_date=%s&end_date=%s" % (start, end)

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
        
        return df_stats
    
    def tabla_geo_stats(self, start, end) :
        endpoint = ("https://api.sendgrid.com/v3/geo/stats?start_date=%s&end_date=%s" % (start, end))

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
               
        return df_gstats  
    
if __name__ == "__main__"    : 
    env()
    messages_table = "messages"
    stats_table = "stats"
    gstats_table = "geo_stats"
    
    engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format( user = os.environ['REDSHIFT_USER'], 
                                                                                            contr= os.environ["REDSHIFT_PASSWORD"],
                                                                                            port= os.environ["REDSHIFT_PORT"],
                                                                                            base= os.environ["REDSHIFT_DB"], 
                                                                                            host= os.environ["REDSHIFT_HOST"] ), 
                               connect_args={'options': '-csearch_path={schema}'.format( schema = os.environ["REDSHIFT_SCHEMA"] )}, echo = False)
                


    tablas = sendgrid()

    end_date = datetime.today()
    
    start_date = end_date - timedelta(days = 1)
    
    df_mess = tablas.tabla_mensajes(start_date, end_date) 
    
    
    end_date = date.today()
    
    start_date = end_date - timedelta(days = 10)
    
    
    
    df_stats = tablas.tabla_stats(start_date, end_date)
    df_gstats = tablas.tabla_geo_stats(start_date, end_date)
    
    #Elimina las tablas en el esquema de redshift (si existen) y las crea con la columna "column"
    Initialize(name = messages_table, engine = engine, column = "msg_id", d_type = "character varying (65535)")
    Initialize(name = stats_table, engine = engine, column = "date", d_type = "DATE")
    Initialize(name = gstats_table, engine = engine, column = "date", d_type = "DATE")


    #Inserta las columnas nuevas en las bases de datos    
    New_Columns(tabla = df_mess, eng = engine, name = messages_table)
    New_Columns(tabla = df_stats, eng = engine, name = stats_table)
    New_Columns(tabla = df_gstats, eng = engine, name = gstats_table)
    
    #Se guardan los CSV
    df_mess.to_csv("messages.csv", index=False)
    df_stats.to_csv("stats.csv", index=False)
    df_gstats.to_csv("geo_stats.csv", index=False)
    
    #Se cargan los CSV a S3
    Upload_S3(["messages.csv","stats.csv","geo_stats.csv"])

    #Se hace el COPY a redshift
    Copy_Redshift('messages.csv', messages_table, engine)
    Copy_Redshift('stats.csv', stats_table, engine)
    Copy_Redshift('geo_stats.csv', gstats_table, engine)


       