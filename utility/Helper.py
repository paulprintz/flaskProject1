##############################################################################################
# File download
import requests
import re
import os

from datetime import datetime
# This function translate date into milliseconds, by multiply 1000 to unix timestamp.
# Because on the server side, it expects milli seconds rather than seconds.
def toUnixTimeStampMilli(date_time="1970-1-1"):
    dt=datetime.strptime(date_time,"%Y-%m-%d") #%H:%M:%S")
    timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
    #print(timestamp)
    return int(timestamp*1000)
#toUnixTimeStamp("2021-4-19 00:00:00")

url='https://api.school.shedusoft.com/apistem/export/db/stat_course_access_records_last'
def saveDataFile(url,starttime:str=None,endtime:str=None):
    savedFilePath=""
    jsonData={"responsetype":"file"}
    if not (starttime is None or endtime is None):
        jsonData['starttime']=toUnixTimeStampMilli(starttime).__str__()
        jsonData['endtime']=toUnixTimeStampMilli(endtime).__str__()
    response = requests.post(url,data=jsonData) # (your url)
    print(response.status_code)
    if response.status_code==200:
        data = response.content
        d = response.headers['content-disposition']
        fname = re.findall("filename=(.+)", d)[0]
        savedFilePath=os.path.join('d:\\temp',fname)
        with open(savedFilePath, 'wb') as f:
            f.write(data)
    else:
        print("failed to save.")
    print(savedFilePath)
    return savedFilePath

url_users='''https://api.school.shedusoft.com/apistem/export/users'''
url_dict={'每用户每活动每次':'''https://api.school.shedusoft.com/apistem/export/db/stat_activity_access_records?appid=stem''',
          '作业':'''https://api.school.shedusoft.com/apistem/export/user_class_assignments?appid=stem'''}
# saveDataFile(url_log,starttime="2020-1-1",endtime="2021-4-19")
