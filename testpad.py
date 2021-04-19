# 增量备份 StudentWorks
import pandas as pd
#from bs4 import BeautifulSoup
import utility.Helper as helper
from datetime import datetime
enddate = datetime.now().date().__str__()
url = helper.url_dict['作业']
saved_file_path = helper.saveDataFile(url, starttime="2021-1-1", endtime=enddate)
# 1 获取所有StudentWorks, 作业
works_df=pd.read_excel(saved_file_path) # "d:\\temp_data\\作业.xlsx")
works_df=works_df[['用户id', '班级id', '学习活动id', '上传附件', '最后提交时间']]
works_df.columns=['UserID','ClassID','ActivityID','Attachment','SubmitTime']
# 2 获取数据库中已有StudentWorks
from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String
engine=create_engine('mssql+pymssql://sa:111111@localhost/LSS',echo=True)
conn=engine.connect()

meta = MetaData()
enrollment = Table(
   'StudentWorks', meta,
   Column('UserID', Integer, primary_key=True),
   Column('ClassID', Integer, primary_key=True),
   Column('ActivityID', Integer, primary_key=True),
   Column('Attachment', String),
   Column('ContentText', String),
   Column('SubmitTime', String)
)
query = enrollment.select()
result = conn.execute(query)
existing_studentWorks_df=pd.read_sql_query(query,conn) # 数据库中已有StudentWorks
# 3 找出新的StudentWorks
merged=works_df.merge(existing_studentWorks_df,how='left', indicator=True)
new_studentWorks=merged[merged['_merge']=='left_only'] # 新的StudentWorks
# 4 将新的Enrollment存入数据库
for index, row in new_studentWorks.iterrows():
    query='''select Classes.ClassID,UserID,Activities.ActivityID  
    from Classes inner join Courses on Classes.CourseID=Courses.ID
    inner join Activities on Courses.ID=Activities.CourseID
    inner join Enrollment on Classes.ClassID=Enrollment.ClassID
    where  Classes.ClassID={} and Enrollment.UserID={} and Activities.ActivityID={}
    '''.format(row['ClassID'],row['UserID'],row['ActivityID'])
    df_class_student_activity=pd.read_sql_query(query,conn)
    if len(df_class_student_activity)>0:
        # ContentText has to be skipped, because it contains sql code.
        insert_sql = '''INSERT INTO StudentWorks(ClassID,UserID,ActivityID,Attachment,SubmitTime)
        VALUES({},{},{},N'{}',N'{}') '''.format(row['ClassID'],row['UserID'],row['ActivityID'],
                                                   row['Attachment'],row['SubmitTime'])
        print(insert_sql)
        engine.execute(insert_sql)
# 完成


##############################################################
import requests
url = "https://api.school.shedusoft.com/apistem/export/db/stat_course_access_records_last"
payload="{\"responsetype\":\"file\",\"starttime\": \"1609459200000\",\"endtime\": \"1640995200000\"}"
headers = {
  'Content-Type': 'application/json'
}
response = requests.request("POST", url, headers=headers, data=payload)
print(response.text)
#############################################################################

# savedFilePath=""
# url='''https://api.school.shedusoft.com/apistem/export/user_class_assignments?appid=stem'''
# jsonData={}#{"responsetype":"file"}
# jsonData['starttime']=1609459200000
# jsonData['endtime']=1640995200000
# response = requests.post(url,data=jsonData) # (your url)
# print(response.status_code)