#import datetime

##################################################################################################

from flask import Flask,render_template,session,redirect, url_for, send_from_directory,send_file,make_response,request,jsonify
from datetime import datetime

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField,FileField,TextAreaField,HiddenField,RadioField,PasswordField,validators
from wtforms.validators import DataRequired,InputRequired,Length

from flask_bootstrap import Bootstrap

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pandas as pd

import os

meta=MetaData()
solutions=Table(
    'solutions',meta,
    Column('ActivityID',Integer),
    Column('PostText',String),
    Column('SolutionID',Integer)
)
tinymceValue='''<p>import random<br />words=["elephant","horse","cat","rabbit","dog","giraffe"]<br />print("Guess the word in my head, letter by letter.")<br />print("If you got six error, you will be hanged by the neck!")<br />secretword=words[random.randint(0,len(words)-1)]<br />guessedletters=[]<br />outprintstring=""<br />ierrorcount=0<br />while True:<br />&nbsp; &nbsp; outprintstring=""<br />&nbsp; &nbsp; for letter in secretword:<br />&nbsp; &nbsp; &nbsp; &nbsp; if guessedletters.__contains__(letter):<br />&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; outprintstring += letter<br />&nbsp; &nbsp; &nbsp; &nbsp; else:<br />&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; outprintstring += '_'<br />&nbsp; &nbsp; if not outprintstring.__contains__("_"):<br />&nbsp; &nbsp; &nbsp; &nbsp; print("You slipped through my fingers! But next time I'll hang you!")<br />&nbsp; &nbsp; &nbsp; &nbsp; break<br />&nbsp; &nbsp; print("The word is :",outprintstring)<br />&nbsp; &nbsp; gletter = input("Pick a letter:")<br />&nbsp; &nbsp; guessedletters.append(gletter)<br />&nbsp; &nbsp; if not secretword.__contains__(gletter):<br />&nbsp; &nbsp; &nbsp; &nbsp; ierrorcount += 1<br />&nbsp; &nbsp; &nbsp; &nbsp; print("You've got {0} out of 6 errors.".format(ierrorcount))<br />&nbsp; &nbsp; if ierrorcount == 6:<br />&nbsp; &nbsp; &nbsp; &nbsp; print("You are hanged by the neck!")<br />&nbsp; &nbsp; &nbsp; &nbsp; break</p>'''
ins=solutions.insert().values(ActivityID=13460,PostText=tinymceValue)

engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
conn = engine.connect()

conn.execute(ins)
###############################################################################################################

from flask import Flask,render_template,session,redirect, url_for, send_from_directory,send_file,make_response,request,jsonify
from datetime import datetime

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField,FileField,TextAreaField,HiddenField,RadioField,PasswordField,validators
from wtforms.validators import DataRequired,InputRequired,Length

from flask_bootstrap import Bootstrap

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pandas as pd

import os
from werkzeug.security import generate_password_hash,check_password_hash

stdtime='2021-03-30 23:30:19.000'
dt=datetime.strptime(stdtime,'%Y-%m-%d %H:%M:%S.000')
dt.isocalendar()[1] #week of year

def weekofyear(stdtime:str):
    dt = datetime.strptime(stdtime, '%Y-%m-%d %H:%M:%S')
    return dt.isocalendar()[1]  # week of year

name = 'klay'
classID=370
engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
conn = engine.connect()

query = '''select ClassName from Classes where ClassID={}'''.format(classID)
className = pd.read_sql_query(query, conn)
# Get UserID by UserName
query = '''
        select UserID from Users where USERNAME='{}'\
        '''.format(name)
user_df = pd.read_sql_query(query, conn)
userID = user_df.values[0].item()
print(userID)
# classID=334, userID=7765
query = """
        declare @ClassID as int
        declare @UserID as int
        set @ClassID={}
        set @UserID={}
        select r1.*,r2.RowNum,solutions_r3.SolutionCount from 
        (
            select userActivities.*,classActivities.PersonCompleted from
            (
                select Activities.ActivityID,SubmitTime,ActivityName,UserID, SelfCheck, SelfCheckDateTime from StudentWorks inner join Activities on StudentWorks.ActivityID=Activities.ActivityID
                where ClassID=@ClassID and UserID=@UserID
            ) as userActivities
            inner join (
                select ActivityID, count(ActivityID) as PersonCompleted 
                from StudentWorks
                where ClassID=@ClassID 
                group by ActivityID
            ) as classActivities on userActivities.ActivityID=classActivities.ActivityID
        ) as r1 inner join
        (
            select * from
            (
                select ROW_NUMBER() over(Partition By ActivityID order by SubmitTime) as RowNum, UserID,ActivityID,SubmitTime from StudentWorks
                where ClassID=@ClassID 
            ) as r1 where UserID=@UserID

        ) as r2 on r1.UserID=r2.UserID and r1.ActivityID=r2.ActivityID
		left outer join 
		(select ActivityID,Count(SolutionID) as SolutionCount from Solutions group by ActivityID) as solutions_r3 on r2.ActivityID=solutions_r3.ActivityID
        order by SubmitTime
        """.format(classID, userID)  # 370，8060
studentWorks_df = pd.read_sql_query(query, conn)
studentWorks_df['SolutionCount'].fillna(0, inplace=True)
studentWorks_df['SolutionCount'] = studentWorks_df['SolutionCount'].astype(int)

# Create Attendance Points
studentWorks_df['WeekNumofYear']=studentWorks_df.apply(lambda x:weekofyear(str(x['SubmitTime'])),axis=1 )
tmpdf=studentWorks_df.groupby('WeekNumofYear').count()[['ActivityID']]
# return render_template(
#     'works.html',
#     title='Student Works',
#     className=str(className.values[0][0]),
#     year=datetime.now().year,
#     name=name,
#     column_names=studentWorks_df.columns.values, row_data=list(studentWorks_df.values.tolist()),
#     column_names_ch=["活动编号", "完成时间", "活动名称", "用户编号", "自我检查", "检查日期", "完成人数", "完成名次", "参考答案数量"],
#     zip=zip
# )

########################################################################################
# 练习完成个数
query='''select UserID,count(ActivityID) as Completed from StudentWorks where ClassID=370
group by UserID'''
activities_df=pd.read_sql_query(query,conn)
import seaborn as sns
import matplotlib.pyplot as plt
# sns.displot(activities_df['Completed'],bins=20)
# sns.boxplot(activities_df['Completed'])
# sns.violinplot(activities_df['Completed'],split=True, inner="quartile")
# sns.swarmplot(activities_df['Completed'])
ax=sns.kdeplot(activities_df['Completed'] )#,cumulative=True) # , bw_adjust=5, cut=0)
y=ax.lines[0].get_ydata()
mode_idx=y.argmax()
ax.vlines(53,0,y[mode_idx],color='crimson',ls=':')
plt.show()


########################################################################################
# 期末教学数据整理：查找没有学习数据的学生
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pandas as pd
engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
conn = engine.connect()
query='''
select distinct Users.* from Users inner join StudentWorks on Users.UserID=StudentWorks.UserID 
where StudentWorks.ClassID=370
'''
active_students_df=pd.read_sql_query(query,conn) # 有学习记录的学生
et_students_df:pd.DataFrame=pd.read_excel("D:\\SynologyDrive\\Drive\\_Teaching\\Python and Machine Learning\\June-1-2021\\数据库基础与应用II2020~2021第二学期 教育技术本科班名单.xlsx") #教育技术班名单
et_students_status_df=et_students_df.merge(active_students_df, how="left", left_on=['姓名'],right_on=['RealName'],indicator=True)
confirm_df=et_students_status_df[['学号/工号','姓名','RealName']]