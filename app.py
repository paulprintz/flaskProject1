"""
Routes and views for the flask application.
"""
from flask import Flask,render_template,session,redirect, url_for
from datetime import datetime

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

from flask_bootstrap import Bootstrap

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pandas as pd

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hard to guess string'
bootstrap = Bootstrap(app)

class NameForm(FlaskForm):
    name = StringField('What is your name?', validators=[DataRequired()])
    submit = SubmitField('Submit')



@app.route('/', methods=['GET','POST'])
@app.route('/home')
def home():
    name=session.get('name') #None
    form=NameForm()
    if form.validate_on_submit():
        name=form.name.data
        session['name']=name

    engine = create_engine('mssql+pymssql://sa:111111@localhost/LSS', echo=True)
    conn = engine.connect()
    query="""
            select StudentCourses.*,ActivityCount from 
        (
        select Courses.ID as CourseID, Courses.Name +'- ' + Classes.ClassName as FullClassName
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
        year=datetime.now().year,
        form=form,
        name=name,
        column_names=courses_df.columns.values, row_data=list(courses_df.values.tolist()), zip=zip
    )

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

if __name__ == '__main__':
    app.run()