import boto3
import pandas as pd
import pandas_redshift as pr
from datetime import datetime, date, timedelta
from boto3 import Session
import json
import os

s3client = Session().client('s3')
s3 = boto3.resource('s3')

def exists(bucket: str, key: str) -> bool:
    contents = s3client.list_objects(Prefix=key, Bucket=bucket).get("Contents")
    if contents:
        for content in contents:
            if content.get("Key") == key:
                return True
    return False

def cleansing_format_data(date):
    colss_li = [i for i in range(0,211)]
    del colss_li[11:21]
    colss_li.remove(2)
    colss_li.remove(7)

    response = s3client.list_objects(
        Bucket='ld-rawdata-2',
        Prefix= 'TR_JISSEKI/' + date + 'XXXXXX/'
    )

    if 'Contents' in response:
        keys = [content['Key'] for content in response['Contents']]
        key = keys[-1] #２３時のデータ

    bucket_name = 'ld-rawdata'
    file_name = key
    day = file_name[37:45] #day string
    reader = pd.read_csv('s3n://'+bucket_name+'/'+file_name,
            encoding="cp932",
            header=None,
            iterator=True,
            chunksize=1000,
            usecols=colss_li)
    df = pd.concat((r for r in reader), ignore_index=True)

    li = []
    df = df[df[0].isin([day])]
    hour = 7
    base = df.loc[:, 0:10]

    #Make hours list
    for i in range(19):
        if hour < 24:
            base.loc[:, 0] = pd.datetime(int(day[0:4]),int(day[4:6]),int(day[6:8]), hour)
        elif hour > 23:
            base.loc[:, 0] = pd.datetime(int(day[0:4]),int(day[4:6]),int(day[6:8]), hour-24)+timedelta(days=1)

        hour+=1
        li.append(pd.concat([base, df.loc[:, 21+i*10:30+i*10]], axis=1))

    #set columns
    for i in range(len(li)):
        li[i].columns = [j for j in range(19)]


    df3 = pd.concat(li) #concat li. df3 is final dataframe
    df3 = df3[[0,1,2,3,4,5,6,9,10,11,12,13,14,15,16,17]]
    df3.columns = ['date_ymdh', 'ten_cd', 'sku_cd', 'dpt_cd', 'line_cd', 'class_cd', 'sku_name', 'urisu', 'urikin', 'gsagsu1', 'gsaggk1', 'gsagsu2', 'gsaggk2', 'gsagsu3', 'gsaggk3', 'garari']

    dbname = os.getenv('REDSHIFT_DB')
    host = os.getenv('REDSHIFT_HOST')
    port = os.getenv('REDSHIFT_PORT')
    user = os.getenv('REDSHIFT_USER')
    password = os.getenv('REDSHIFT_PASS')

    pr.connect_to_redshift(dbname = dbname,
                            host = host,
                            port = port,
                            user = user,
                            password = password)

    pr.connect_to_s3(aws_access_key_id = os.getenv('ACCESS_KEY_ID'),
                    aws_secret_access_key = os.getenv('SECRET_ACCESS_KEY'),
                    bucket = 'nichiji-tmp'
                    )

    pr.pandas_to_redshift(data_frame = df3,
                            redshift_table_name = 'jisseki_nichiji',
                            append = True)

if __name__ == '__main__':
    start_day = os.environ['START_DAY']
    end_day = os.environ['END_DAY']

    BUCKET_NAME = 'nichiji-tmp'
    OBJECT_KEY_NAME = start_day+'_'+end_day+'.txt'

    if exists(BUCKET_NAME, OBJECT_KEY_NAME) != True:
        s3.Bucket(BUCKET_NAME).put_object(Key=OBJECT_KEY_NAME)

    start = datetime.strptime(start_day, '%Y%m%d')
    end = datetime.strptime(end_day, '%Y%m%d')


    def daterange(start_date, end_date):
        for n in range((end_date - start_date).days):
            yield start_date + timedelta(n)

    dates = []
    for i in daterange(start, end):
        y = str(i.year)
        m = str(i.month)
        d = str(i.day)
        if len(m) !=2:
            m = '0' + m
        if len(d) != 2:
            d = '0' + d

        dates.append(y+m+d)

    while(1):
        obj = s3.Object(BUCKET_NAME, OBJECT_KEY_NAME)
        key = obj.get()['Body'].read().decode('utf-8')
        tmp_dates = key.split('\n')
        tmp_dates = tmp_dates[:-1]
        if len(dates) == len(tmp_dates):
          break

        in_date = ''
        for v in dates:
            if v in tmp_dates:
                in_date +=v+'\n'
            else:
                cleansing_format_data(v)
                in_date+=v+'\n'
                obj.put(Body = in_date)
                break
