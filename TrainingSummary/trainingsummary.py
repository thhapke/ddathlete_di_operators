from datetime import datetime
import math
import sys

import pytz

import pandas as pd


def on_input(msg):
    
    att = dict(msg.attributes)
    
    header = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=header)
    df['TIMESTAMP'] = pd.to_datetime(df.TIMESTAMP)
    
    df['INDEX'] = df['TIMESTAMP']
    df = df.set_index('INDEX')
    #df.tz_localize('utc')
    df = df.tz_convert(None)
    
    log('Process {} of {} - records:{}'.format(att['table_name'],att['year'],df.shape[0] ))
    
    if att['table_name'] in ['SWIMMING_POOL','SWIMMING_OPEN_WATER'] :
        df['TEMPERATURE'] = -273
    if att['table_name'] == "CYCLING_INDOOR" :
        df['DISTANCE'] = 0

    aggreg = {'DATE':'first','TIMESTAMP':['min','max'],\
              'DISTANCE':['min','max'],\
              'POWER':['min','max','mean'],'HEAR_TRATE':['min','max','mean'],\
              'CADENCE':['min','max','mean'],'TEMPERATURE':'max'}
    tdf = df.groupby('TRAINING_ID').agg(aggreg).reset_index()
    tdf.columns = ['_'.join(col).upper() for col in tdf.columns]
    
    
    tdf['SPORT_TYPE'] = att['table_name']
    tdf['TRAINING_TYPE'] = 'Unknown'
    tdf['DISTANCE'] = tdf['DISTANCE_MAX'] - tdf['DISTANCE_MIN']
    tdf.rename(columns={"TRAINING_ID_": "TRAINING_ID", "DATE_FIRST":"DATE", 
                        "TEMPERATURE_MAX":"TEMPERATURE"},inplace = True)
                        
    tdf['TRAINING_NO'] = 1
    tdf['TRAINING_NO'] = tdf.groupby('DATE')['TRAINING_NO'].cumsum()
                        
    
    tdf['DURATION'] = (tdf['TIMESTAMP_MAX'] - tdf['TIMESTAMP_MIN']).dt.total_seconds()
    tdf['TIMESTAMP_START'] = tdf['TIMESTAMP_MIN'].dt.strftime('%Y-%m-%d %H:%M:%S')
    tdf['TIMESTAMP_END'] = tdf['TIMESTAMP_MAX'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # cast 
    tdf['DISTANCE'] =  tdf['DISTANCE'].astype('float')
    tdf['DURATION'] =  tdf['DURATION'].astype('int')
    tdf['HEART_RATE_MIN'] =  tdf['HEART_RATE_MIN'].astype('int')
    tdf['HEART_RATE_MAX'] =  tdf['HEART_RATE_MAX'].astype('int')
    tdf['HEART_RATE_MEAN'] =  tdf['HEART_RATE_MEAN'].astype('int')
    tdf['CADENCE_MIN'] =  tdf['CADENCE_MIN'].astype('float')
    tdf['CADENCE_MAX'] =  tdf['CADENCE_MAX'].astype('float')
    tdf['CADENCE_MEAN'] =  tdf['CADENCE_MEAN'].astype('float')
    tdf['POWER_MIN'] =  tdf['POWER_MIN'].astype('float')
    tdf['POWER_MAX'] =  tdf['POWER_MAX'].astype('float')
    tdf['POWER_MEAN'] =  tdf['POWER_MEAN'].astype('float')

    # sort dataframe according to target table
    tdf = tdf[['TRAINING_ID','DATE','SPORT_TYPE','TRAINING_NO','TRAINING_TYPE','DISTANCE','DURATION',\
             'TIMESTAMP_START','TIMESTAMP_END','POWER_MIN','POWER_MAX','POWER_MEAN','HEART_RATE_MIN','HEART_RATE_MAX', \
             'HEART_RATE_MEAN','CADENCE_MIN','CADENCE_MAX','CADENCE_MEAN','TEMPERATURE']]
    
    log('Length of df: {}'.format(len(tdf)))
    
    att = msg.attributes
    att["table"] = {"columns":[
                    {"class":str(tdf[tdf.columns[0]].dtype),"tdf_name":tdf.columns[0],"name":"TRAINING_ID","nullable":False,"type":{"hana":"BIGINT"}},
                    {"class":str(tdf[tdf.columns[1]].dtype),"tdf_name":tdf.columns[1],"name":"DATE","nullable":True,"type":{"hana":"DAYDATE"}},
                    {"class":str(tdf[tdf.columns[2]].dtype),"tdf_name":tdf.columns[2],"name":"SPORT_TYPE","nullable":False,"size":25,"type":{"hana":"NVARCHAR"}},
                    {"class":str(tdf[tdf.columns[3]].dtype),"tdf_name":tdf.columns[3],"name":"TRAINING_NO","nullable":True,"type":{"hana":"INTEGER"}},
                    {"class":str(tdf[tdf.columns[4]].dtype),"tdf_name":tdf.columns[4],"name":"TRAINING_TYPE","nullable":True,"size":25,"type":{"hana":"NVARCHAR"}},
                    {"class":str(tdf[tdf.columns[5]].dtype),"tdf_name":tdf.columns[5],"name":"DISTANCE","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[6]].dtype),"tdf_name":tdf.columns[6],"name":"DURATION","nullable":True,"type":{"hana":"INTEGER"}},
                    {"class":str(tdf[tdf.columns[7]].dtype),"tdf_name":tdf.columns[7],"name":"TIMESTAMP_START","nullable":True,"type":{"hana":"LONGDATE"}},
                    {"class":str(tdf[tdf.columns[8]].dtype),"tdf_name":tdf.columns[8],"name":"TIMESTAMP_END","nullable":True,"type":{"hana":"LONGDATE"}},
                    {"class":str(tdf[tdf.columns[9]].dtype),"tdf_name":tdf.columns[9],"name":"POWER_MIN","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[10]].dtype),"tdf_name":tdf.columns[10],"name":"POWER_MAX","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[11]].dtype),"tdf_name":tdf.columns[11],"name":"POWER_MEAN","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[12]].dtype),"tdf_name":tdf.columns[12],"name":"HEARTRATE_MIN","nullable":True,"type":{"hana":"INTEGER"}},
                    {"class":str(tdf[tdf.columns[13]].dtype),"tdf_name":tdf.columns[13],"name":"HEARTRATE_MAX","nullable":True,"type":{"hana":"INTEGER"}},
                    {"class":str(tdf[tdf.columns[14]].dtype),"tdf_name":tdf.columns[14],"name":"HEARTRATE_MEAN","nullable":True,"type":{"hana":"INTEGER"}},
                    {"class":str(tdf[tdf.columns[15]].dtype),"tdf_name":tdf.columns[15],"name":"CADENCE_MIN","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[16]].dtype),"tdf_name":tdf.columns[16],"name":"CADENCE_MAX","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[17]].dtype),"tdf_name":tdf.columns[17],"name":"CADENCE_MEAN","nullable":True,"type":{"hana":"DOUBLE"}},
                    {"class":str(tdf[tdf.columns[18]].dtype),"tdf_name":tdf.columns[18],"name":"TEMPERATURE","nullable":True,"type":{"hana":"DOUBLE"}}],"name":"TRAINING_SUMMARY","version":1}


    data = tdf.values.tolist()
    api.send("output", api.Message(attributes = att,body = data))
    


api.set_port_callback("input", on_input)

