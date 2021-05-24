################################################################################################################
# 增量备份 Users
def extractUsers():
    import pandas as pd
    import utility.Helper as helper
    from datetime import datetime
    enddate=datetime.now().date().__str__()
    url=helper.url_users
    users_file=helper.saveDataFile(url,starttime="2021-1-1",endtime=enddate)
    # 1 获取学习平台的所有Users
    users_df=pd.read_excel(users_file) #"d:\\temp_data\\用户.xlsx")
    users_df=users_df[users_df['用户名'].notna()] # 过滤掉用户名为NaN的Users
    users_df.columns=['UserID','UserName','Phone','SSID','RealName','EMail','RegisterDateTime']
    users_df=users_df.astype(dtype= {"UserID":"int64",'UserName':"str",'Phone':"str",'SSID':"str",'RealName':"str",'EMail':"str",'RegisterDateTime':"str"})
    # 2 获取数据库中已有的Users
    from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String
    engine=create_engine('mssql+pymssql://sa:111111@localhost/LSS',echo=True)
    conn=engine.connect()

    meta = MetaData()
    users = Table(
       'Users', meta,
       Column('UserID', Integer, primary_key=True),
       Column('UserName', Integer),
       Column('Phone', String),
       Column('SSID', String),
       Column('RealName',String),
       Column('EMail',String),
       Column('RegisterDateTime',String)
    )
    query = users.select()
    result = conn.execute(query)
    existing_users_df=pd.read_sql_query(query,conn) # 数据库中已有Users
    existing_users_df=existing_users_df.astype(dtype= {"UserID":"int64",'UserName':"str",'Phone':"str",'SSID':"str",'RealName':"str",'EMail':"str",'RegisterDateTime':"str"})
    # 3 找出新的Users
    #merged=users_df.merge(existing_users_df,how='left', indicator=True)
    merged=users_df.merge(existing_users_df,how='left', indicator=True)
    new_users=merged[merged['_merge']=='left_only'] # 新的Users
    # 4 将新的Users存入数据库
    for index, row in new_users.iterrows():
        query='''select * from Users where UserID={}'''.format(row['UserID'])
        df=pd.read_sql_query(query,conn)
        if len(df)==0:
            sql = '''INSERT INTO Users(UserID, UserName, Phone, SSID, RealName, EMail,RegisterDateTime)
             VALUES({},N'{}',N'{}',N'{}',N'{}',N'{}',N'{}') '''.\
                format(row['UserID'], row['UserName'],row['Phone'],row['SSID'],row['RealName'],row['EMail'],row['RegisterDateTime'])
        elif len(df)==1:
            sql='''Update Users set UserName=N'{}',Phone='{}',SSID='{}',RealName=N'{}',Email='{}',RegisterDateTime='{}'
            where UserID={}'''.format(row['UserName'],row['Phone'],row['SSID'],row['RealName'],row['EMail'],row['RegisterDateTime'],row['UserID'],)
        print(sql)
        engine.execute(sql)
    # 完成
# extractUsers()
################################################################################################################
def extractCoursesActivitiesClassesEnrollment():
    # 开始：这段代码是增量存储课程名称和ID
    import pandas as pd
    import pandas as pd
    import utility.Helper as helper
    from datetime import datetime
    enddate = datetime.now().date().__str__()
    url = helper.url_dict['每用户每活动每次']
    saved_file_path = helper.saveDataFile(url, starttime="2021-1-1", endtime=enddate)
    # 1 获取学习平台的所有课程名称和ID
    df=pd.read_excel(saved_file_path) # "d:\\temp_data\\学习记录-每用户每活动每次.xlsx")
    courses_df=df[['课程id', '课程名称']].drop_duplicates()
    courses_df.columns=['ID','Name'] # 所有课程名称与ID
    courses_df=courses_df[courses_df['Name']!='undefined'] # 有ID相同Name为undifined的情况。
    def getCourseName(row):
        courseID=row['ID']
        courses=courses_df[courses_df['ID']==courseID]
        if len(courses)==1: # 没有ID重复
            return row
        else: # 有多个Name
            subset=df[df['课程id']==courseID].sort_values(by=['进入时间(时间戳)'],ascending=False)
            row['Name']=subset.iloc[:1]['课程名称'].values[0] # 按时间取最新的Name
            return row
    courses_df=courses_df.apply(getCourseName,axis=1)
    courses_df= courses_df.drop_duplicates(ignore_index=True)

    # 2 获取数据库中的已有课程名称和ID
    from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String
    engine=create_engine('mssql+pymssql://sa:111111@localhost/LSS',echo=True)
    conn=engine.connect()

    meta = MetaData()
    courses = Table(
       'courses', meta,
       Column('ID', Integer, primary_key = True),
       Column('Name', String)
    )
    query = courses.select()
    #conn = engine.connect()
    result = conn.execute(query)
    # for row in result:
    #    print (row)
    df1=pd.read_sql_query(query,conn) # 数据库中已有课程名称和ID
    # 3 找出新的课程名称和ID
    merged=courses_df.merge(df1,how='left', indicator=True)
    new_courses=merged[merged['_merge']=='left_only'] # 新的课程名称和ID
    # 4 将新的课程名称和ID存入数据库
    for index, row in new_courses.iterrows():
        query = '''select * from Courses where ID={}'''.format(row['ID'])
        df = pd.read_sql_query(query, conn)
        if len(df) == 0:
            sql = '''INSERT INTO Courses(ID, Name) VALUES({},N'{}') '''.format(row['ID'], row['Name'])
        elif len(df)==1:
            sql='''update Courses set Name=N'{}' where ID={}
            '''.format(row['Name'],row['ID'])
        print(sql)
        engine.execute(sql)
    # 完成
    ############################################
    # 增量备份 Activities
    # 获取所有Activities, 消除冗余和名称不一致。
    activities_df=df[['课程id','学习活动id', '学习活动名称','学习活动类型']].drop_duplicates()
    activities_df.columns=['CourseID','ActivityID','ActivityName','ActivityType']
    activities_df=activities_df[activities_df['ActivityName']!='[object Object]']
    activities_df=activities_df.sort_values(by=['CourseID','ActivityID']) # For debugging only
    def getActivityName(row):
        courseID=row['CourseID']
        activityID=row['ActivityID']
        activities=activities_df[(activities_df['CourseID']==courseID) & (activities_df['ActivityID']==activityID)]
        if len(activities)==1: # 没有多个Name
            return row #activities['ActivityName']
        else: # 有多个Name
            subset=df[(df['课程id']==courseID) & (df['学习活动id']==activityID)].sort_values(by=['进入时间(时间戳)'],ascending=False)
            row['ActivityName']=subset.iloc[:1]['学习活动名称'].values[0] # 按时间取最新的Name
            return row
    activities_df=activities_df.apply(getActivityName,axis=1)
    activities_df= activities_df.drop_duplicates(ignore_index=True)

    # 2 获取数据库中的已有Activities
    from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String
    engine=create_engine('mssql+pymssql://sa:111111@localhost/LSS',echo=True)
    conn=engine.connect()

    meta = MetaData()
    activities = Table(
       'Activities', meta,
       Column('CourseID', Integer, primary_key=True),
       Column('ActivityID', Integer, primary_key=True),
       Column('ActivityName', String),
       Column('ActivityType', String)
    )
    query = activities.select()
    #conn = engine.connect()
    result = conn.execute(query)
    # for row in result:
    #    print (row)
    existing_activities_df=pd.read_sql_query(query,conn) # 数据库中已有Activities
    # 3 找出新的Activities
    merged=activities_df.merge(existing_activities_df,how='left', indicator=True)
    new_activities=merged[merged['_merge']=='left_only'] # 新的Activities
    # 4 将新的Activities存入数据库
    for index, row in new_activities.iterrows():
        insert_sql = '''INSERT INTO Activities(CourseID, ActivityID,ActivityName,ActivityType) VALUES({},{},N'{}',N'{}') '''.\
            format(row['CourseID'], row['ActivityID'],row['ActivityName'],row['ActivityType'])
        print(insert_sql)
        engine.execute(insert_sql)
    # 完成

    ###############################################################################################################
    # 增量备份 Classes
    # 获取所有Classes, 消除冗余和名称不一致。
    classes_df=df[['课程id','班级id', '班级名称']].drop_duplicates()
    classes_df.columns=['CourseID','ClassID','ClassName']

    # 2 获取数据库中的已有Classes
    from sqlalchemy import create_engine,MetaData, Table, Column, Integer, String
    engine=create_engine('mssql+pymssql://sa:111111@localhost/LSS',echo=True)
    conn=engine.connect()

    meta = MetaData()
    classes = Table(
       'Classes', meta,
       Column('CourseID', Integer),
       Column('ClassID', Integer, primary_key=True),
       Column('ClassName', String)
    )
    query = classes.select()
    result = conn.execute(query)
    existing_classes_df=pd.read_sql_query(query,conn) # 数据库中已有Classes
    # 3 找出新的Activities
    merged=classes_df.merge(existing_classes_df,how='left', indicator=True)
    new_classes=merged[merged['_merge']=='left_only'] # 新的Activities
    # 4 将新的Activities存入数据库
    for index, row in new_classes.iterrows():
        insert_sql = '''INSERT INTO Classes(CourseID, ClassID,ClassName) VALUES({},{},N'{}') '''.\
            format(row['CourseID'], row['ClassID'],row['ClassName'])
        print(insert_sql)
        engine.execute(insert_sql)
    # 完成
    ###############################################################################################################
    # 增量备份 Enrollment
    import pandas as pd
    # 1 获取学习平台的所有Enrollment, 哪些User参加了哪些Class
    enrollment_df = df[['班级id', '用户id']].drop_duplicates()
    enrollment_df.columns = ['ClassID', 'UserID']
    # 2 获取数据库中已有的Enrollment
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()

    meta = MetaData()
    enrollment = Table(
        'Enrollment', meta,
        Column('UserID', Integer, primary_key=True),
        Column('ClassID', Integer, primary_key=True)
    )
    query = enrollment.select()
    result = conn.execute(query)
    existing_enrollment_df = pd.read_sql_query(query, conn)  # 数据库中已有Enrollment
    # 3 找出新的Users
    merged = enrollment_df.merge(existing_enrollment_df, how='left', indicator=True)
    new_enrollment = merged[merged['_merge'] == 'left_only']  # 新的Enrollment
    # 4 将新的Enrollment存入数据库
    for index, row in new_enrollment.iterrows():
        insert_sql = '''INSERT INTO Enrollment(UserID, ClassID)
         VALUES({},{}) '''.format(row['UserID'], row['ClassID'])
        print(insert_sql)
        engine.execute(insert_sql)
    # 完成
# extractCoursesActivitiesClassesEnrollment()
################################################################################################################
# 增量备份 StudentWorks
def extractStudentWorks():
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
    works_df=works_df.astype(dtype= {'SubmitTime':"datetime64"})
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
    merged=works_df.merge(existing_studentWorks_df,how='left',on=['UserID','ClassID','ActivityID','Attachment','SubmitTime'], indicator=True)
    new_studentWorks=merged[merged['_merge']=='left_only'] # 新的StudentWorks
    print(len(new_studentWorks))
    # 4 将新的StudentWorks存入数据库
    for index, row in new_studentWorks.iterrows():
        query='''select Classes.ClassID,UserID,Activities.ActivityID  
                from Classes inner join Courses on Classes.CourseID=Courses.ID
                inner join Activities on Courses.ID=Activities.CourseID
                inner join Enrollment on Classes.ClassID=Enrollment.ClassID
                where  Classes.ClassID={} and Enrollment.UserID={} and Activities.ActivityID={}
                '''.format(row['ClassID'],row['UserID'],row['ActivityID'])
        df_class_student_activity=pd.read_sql_query(query,conn)
        if len(df_class_student_activity)>0:
            query = '''select * from StudentWorks where classID={} and UserID={} and ActivityID={}''' \
                .format(row['ClassID'],row['UserID'],row['ActivityID'])
            df = pd.read_sql_query(query, conn)
            if len(df) == 0:
                # ContentText has to be skipped, because it contains sql code.
                sql = '''INSERT INTO StudentWorks(ClassID,UserID,ActivityID,Attachment,SubmitTime)
                        VALUES({},{},{},N'{}',N'{}') '''.format(row['ClassID'],row['UserID'],row['ActivityID'],
                                                                row['Attachment'],row['SubmitTime'])
            elif len(df)==1:
                sql='''update StudentWorks set Attachment='{}',SubmitTime='{}' where classID={} and UserID={} and ActivityID={}''' \
                    .format(row['Attachment'],row['SubmitTime'],row['ClassID'],row['UserID'],row['ActivityID'])
            print(sql)
            engine.execute(sql)
    # 完成
# extractStudentWorks()
################################################################################################################
def extractAll():
    extractUsers()
    extractCoursesActivitiesClassesEnrollment()
    extractStudentWorks()