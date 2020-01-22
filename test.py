import os
from env import env
import requests
import urllib.parse
from datetime import date, datetime, timedelta
from pandas.io.json import json_normalize
import pandas as pd

env()

headers = {'authorization': 'Bearer {}'.format(os.environ["SENDGRID_TOKEN"])}

end_date = datetime.today()
    
start_date = end_date - timedelta(days = 1)

start_date = (str(start_date)[:-7])
end_date = (str(end_date))[:-7]

start_date= (start_date.replace(" ","T")) + "Z"
end_date = end_date.replace(" ", "T") + "Z"
print(end_date)

query1 = 'last_event_time between timestamp "%s" and timestamp "%s"' %(start_date, end_date)

print(query1)

query1 = urllib.parse.quote(str(query1))

print(query1)

endpoint1 = "https://api.sendgrid.com/v3/messages?limit=1000&query=%s" % query1

response1 = requests.get(endpoint1,
                        headers=headers)

dat1 = response1.json()

#print(dat)

df1 = json_normalize(dat1['messages'])

print(df1)




end_date = date.today()
    
start_date = end_date - timedelta(days = 10)

endpoint2 = "https://api.sendgrid.com/v3/stats?start_date=%s&end_date=%s" % (start_date, end_date)

response2 = requests.get(endpoint2,
                        headers=headers)


dat2 = response2.json()
df2 = json_normalize(dat2)
print(df2)

temp = []
temp_d = []
for i in dat2 :
    temp_d.append(i['date'])
    stats = json_normalize(i['stats'][0]['metrics'])
    temp.append(stats)

dates = pd.DataFrame({"date":temp_d})

df_stats = pd.concat(temp, sort= False, ignore_index=True)
    
df_stats = pd.concat([dates,df_stats], axis = 1)

print(df_stats)
    




endpoint3 = ("https://api.sendgrid.com/v3/geo/stats?start_date=%s&end_date=%s" % (start_date, end_date))

response3 = requests.get(endpoint3,
                        headers=headers)

dat3 = response3.json()


dates = []
types = []
names = []
metrics = []
for i in dat3 :
   for j in i['stats'] :
       dates.append(i['date'])
       types.append(j['type'])
       names.append(j['name'])
       temp = json_normalize(j['metrics'])
       metrics.append(temp)
       
df_temp = pd.DataFrame({"date":dates,"type":types,"name":names}) 
df_gstats = pd.concat(metrics, sort=False, ignore_index=True)       
df_gstats = pd.concat([df_temp, df_gstats], axis = 1)
       
print(df_gstats)    
       