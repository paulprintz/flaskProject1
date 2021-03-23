"""
Routes and views for the flask application.
"""
from flask import Flask,render_template,session,redirect, url_for, send_from_directory,send_file,make_response,request,jsonify
from datetime import datetime

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField,FileField,TextAreaField,HiddenField
from wtforms.validators import DataRequired,InputRequired,Length

from flask_bootstrap import Bootstrap

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pandas as pd

import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hard to guess string'
app.config["UPLOAD_FOLDER"]='upload'
bootstrap = Bootstrap(app)

class NameForm(FlaskForm):
    name = StringField('What is your name?', validators=[DataRequired()])
    submit = SubmitField('Submit')

class SolutionForm(FlaskForm):
    title = StringField('Title', validators=[InputRequired(), Length(max=100)])
    post = TextAreaField('Write something')
    tags = StringField('Tags')
    file=FileField()
    activityID=HiddenField('activityID')
    submit=SubmitField('Submit')

@app.route('/logout',methods=['GET'])
def logout():
    session['name']=None
    return redirect(url_for('home'))

@app.route('/', methods=['GET','POST'])
@app.route('/home', methods=['GET','POST'])
def home():
    name=session.get('name') #None
    form=NameForm()
    if form.validate_on_submit():
        name=form.name.data
        session['name']=name
        return redirect(url_for('home'))

    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    query="""
            select StudentCourses.*,ActivityCount from 
        (
        select Courses.ID as CourseID, Courses.Name +'- ' + Classes.ClassName as FullClassName,Classes.ClassID
        from Courses inner join Classes on Courses.ID=Classes.CourseID
        inner join Enrollment on Classes.ClassID=Enrollment.ClassID
        inner join Users on Enrollment.UserID=Users.UserID
        where Users.UserName = '{}'
        ) as StudentCourses left outer join 
        (
            select CourseID, count(ActivityID) as ActivityCount from Activities group by CourseID
        ) as CourseActivityCount on StudentCourses.CourseID=CourseActivityCount.CourseID
        """.format(name)
    courses_df = pd.read_sql_query(query, conn)  # 数据库中已有课程名称和ID

    """Renders the home page."""
    return render_template(
        'index.html',
        title='Home Page',
        # year=datetime.now().year,
        form=form,
        name=name,
        column_names=courses_df.columns.values, row_data=list(courses_df.values.tolist()), zip=zip,
        enumerate=enumerate
    )
# def addIndex(df):
#     return [r for r in zip(range(len(df.values.tolist())),df.values.tolist())]

@app.route('/works/<classID>', methods=['GET'])
def studnet_works(classID):
    name = session.get('name')
    if name is not None:
        engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
        conn = engine.connect()
        # Get UserID by UserName
        query='''
        select UserID from Users where USERNAME='{}'\
        '''.format(name)
        user_df = pd.read_sql_query(query, conn)
        userID=user_df.values[0].item()
        print(userID)
        # classID=334, userID=7765
        query="""
        declare @ClassID as int
        declare @UserID as int
        set @ClassID={}
        set @UserID={}
        select r1.*,r2.RowNum from 
        (
            select userActivities.*,classActivities.PersonCompleted from
            (
                select Activities.ActivityID,SubmitTime,ActivityName,UserID from StudentWorks inner join Activities on StudentWorks.ActivityID=Activities.ActivityID
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
        order by SubmitTime
        """.format(classID,userID)  #7765
        studentWorks_df=pd.read_sql_query(query, conn)
        return render_template(
            'works.html',
            title='Student Works',
            year=datetime.now().year,
            name=name,
            column_names=studentWorks_df.columns.values, row_data=list(studentWorks_df.values.tolist()), zip=zip
        )
@app.route('/courses')
def courses():
    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    query='''
    select Courses.*,courseActivities_r1.AssignmentCount,courseSolutions_r2.SolutionCount from Courses
    left outer join
    (select CourseID, Count(ActivityID) as AssignmentCount from Activities where ActivityType='assignment' group by CourseID) as courseActivities_r1
    on Courses.ID=courseActivities_r1.CourseID left outer join 
    (select CourseID, Count(SolutionID) as SolutionCount from Solutions inner join Activities on Solutions.ActivityID=Activities.ActivityID
    group by CourseID) as courseSolutions_r2
    on Courses.ID=courseSolutions_r2.CourseID'''
    courses_df = pd.read_sql_query(query, conn)
    courses_df.fillna(0,inplace=True)
    courses_df['SolutionCount']=courses_df['SolutionCount'].astype(int)
    courses_df['AssignmentCount'] = courses_df['AssignmentCount'].astype(int)
    return render_template('courses.html',
                           title='Courses',
                           year=datetime.now().year,
                           column_names=courses_df.columns.values, row_data=list(courses_df.values.tolist()),
                           zip=zip
                           )

@app.route('/activities/<courseID>',methods=['GET'])
def activities(courseID):
    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    query='''
    select activities_r1.*,solutions_r2.SolutionCount from
    (select * from Activities where CourseID={} and ActivityType='assignment') as activities_r1
    left outer join
    (select ActivityID, Count(SolutionID) as SolutionCount  from Solutions group by ActivityID) as solutions_r2
    on activities_r1.ActivityID=solutions_r2.ActivityID
    '''.format(courseID)
    courses_df = pd.read_sql_query(query, conn)
    courses_df.fillna(0,inplace=True)
    courses_df["SolutionCount"] = courses_df["SolutionCount"].astype(int)
    return render_template('activities.html',
                           title='Courses',
                           year=datetime.now().year,
                           column_names=courses_df.columns.values, row_data=list(courses_df.values.tolist()),
                           zip=zip
                           )
@app.route('/solution/<activityID>', methods=['GET','POST'])
def solution(activityID):
    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    form=SolutionForm()
    if form.validate_on_submit():
        postText=form.post.data
        activityID=form.activityID.data
        query='''select * from Solutions where activityID={}'''.format(activityID)
        solution_df = pd.read_sql_query(query, conn)
        if len(solution_df)>0:
            query='''update Solutions set PostText=N'{}' where activityID={}'''.format(postText,activityID)
            engine.execute(query)
            #pd.read_sql_query(query, conn)
        else:
            query='''insert into Solutions (ActivityID, PostText) values ({},N'{}')'''.format(activityID,postText)
            engine.execute(query)
            #pd.read_sql_query(query, conn)
        query = '''select * from Solutions where activityID={}'''.format(activityID)
        solution_df = pd.read_sql_query(query, conn)
        return redirect(url_for('solution_details', activityID=solution_df['ActivityID'][0]))
        print(postText)
    query='''
    select ActivityID,Courses.Name as CourseName, ActivityName
    from Activities inner join Courses on Activities.CourseID=Courses.ID
    where ActivityID={}
    '''.format(activityID)
    activity_df = pd.read_sql_query(query, conn)
    form.activityID.data=activityID
    form.title.data=activity_df['ActivityName'][0]

    query = '''select * from Solutions where activityID={}'''.format(activityID)
    solution_df = pd.read_sql_query(query, conn)
    if len(solution_df)>0:
        form.post.data=solution_df['PostText'][0]

    return render_template(
        'solution.html',
        title='Solution',
        form=form,
        activityID=activityID,
        courseName=activity_df['CourseName'][0],
        activityName=activity_df['ActivityName'][0]
    )
@app.route('/solution_details/<activityID>', methods=['GET'])
def solution_details(activityID):
    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    query = '''select Solutions.*,Activities.ActivityName 
    from Solutions inner join Activities on Solutions.ActivityID=Activities.ActivityID
    where Solutions.activityID={}'''.format(activityID)
    solution_df=pd.read_sql_query(query, conn)
    solution_text=""
    activityName=""
    if len(solution_df)>0:
        solution_text=solution_df['PostText'][0]
        activityName=solution_df['ActivityName'][0]
    query='''select CourseID from Activities where ActivityID={}'''.format(activityID)
    activity_df = pd.read_sql_query(query, conn)
    return render_template("solutiondetails.html",
                           post_text=solution_text,
                           activityID=activityID,
                           activityName=activityName,
                           courseID=activity_df['CourseID'][0])

@app.route('/imageuploader', methods=['POST'])
#@login_required
def imageuploader():
    ext=''
    file = request.files.get('file')
    if file:
        filename = file.filename.lower()
        if ext in ['jpg', 'gif', 'png', 'jpeg']:
            img_fullpath = os.path.join(app.config['UPLOADED_PATH'], filename)
            file.save(img_fullpath)
            return jsonify({'location' : filename})

    # fail, image did not upload
    output = make_response(404)
    output.headers['Error'] = 'Image failed to upload'
    return output

@app.route('/contact')
def contact():
    """Renders the contact page."""
    return render_template(
        'contact.html',
        title='Contact',
        year=datetime.now().year,
        message='Your contact page.'
    )

@app.route('/about')
def about():
    """Renders the about page."""
    return render_template(
        'about.html',
        title='About',
        year=datetime.now().year,
        message='Your application description page.'
    )

@app.route("/movies", methods=["GET"])
def get_movie():
    return send_from_directory(
                app.config["UPLOAD_FOLDER"],
                "media1.mp4",
                conditional=True,
            )

# @app.route('/<vid_name>')
# def serve_video(vid_name):
#     vid_path=os.path.join(app.config["UPLOAD_FOLDER"],"media1.mp4")
#     resp = make_response(send_file( vid_path,'video/mp4'))
#     resp.headers['Content-Disposition'] = 'inline'
#     return resp

if __name__ == '__main__':
    app.run(debug=True)


# @app.route('/works/<cID>')
# def works(cID=1):
#     """Renders the contact page."""
#     return render_template(
#         'works.html'
#     )
