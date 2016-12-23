# import necessary libraries
import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool
import time
from engine import RecommendationEngine
from flask import Flask, request, render_template, g, redirect, session, flash, url_for

# create global instance for Recommendation Engine
recommendation_engine = RecommendationEngine()


# create app
tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)

# set secret_key in order to use session
app.secret_key = 'come on, just a simple project'

# set database url and create database engine
DATABASEURI = "sqlite:///test.db"
engine = create_engine(DATABASEURI)

# database setup
engine.execute("""CREATE TABLE IF NOT EXISTS users(
    uid int not null,
    email text not null,
    password text not null)
""")

# start and terminate database connection


@app.before_request
def before_request():
    try:
        g.conn = engine.connect()
    except:
        print "uh oh, problem connecting to database"
        import traceback
        traceback.print_exc()
        g.conn = None


@app.teardown_request
def teardown_request(exception):
    try:
        g.conn.close()
    except Exception as e:
        pass


@app.route('/', methods=['GET', 'POST'])
def homepage():
    uid = ""
    if session.get('logged_in') is None and session.get('uid') is None:
        now = time.localtime()
        for num in now[2:6]:
            uid += str(num)
        session['uid'] = int(uid)
    else:
        pass
    if request.method == 'POST':
        ratings=[int(request.form['ratings0']), int(request.form['ratings1']), int(request.form['ratings2']), int(request.form['ratings3']), int(request.form['ratings4'])]
        rating_list = map(lambda x, y: (session['uid'], x, y), session['music_id'], ratings)
        session.pop('music_id', None)
        print rating_list
        recommendation_engine.add_ratings(rating_list)
    musics = recommendation_engine.get_proposed_ID_and_title(session['uid'])
    print musics
    music = []
    music_id = []
    for row in musics:
        music_id.append(row[0])
        music.append(row[1])
    session['music_id'] = music_id
    return render_template("homepage.html", music=music)


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        cursor = g.conn.execute("SELECT * FROM users WHERE email=? AND password=? LIMIT 1", request.form['email'], request.form['password'])
        user = cursor.fetchone()
        if user == None:
            error = 'Invalid email and password combination!!'
        else:
            session['logged_in'] = True
            session['uid'] = user[0]
            return redirect(url_for('homepage'))
    return render_template("login.html", error=error)


@app.route('/register', methods=['GET', 'POST'])
def register():
    error = None
    uid = ""
    if request.method == 'POST':
        cursor = g.conn.execute("SELECT * FROM users WHERE email=?", request.form['email'])
        if cursor.fetchone() == None:
            cursor.close()
            now = time.localtime()
            for num in now[2:6]:
                uid += str(num)
            session['uid'] = int(uid)
            session['logged_in'] = True
            g.conn.execute("INSERT INTO users VALUES (?,?,?)", session['uid'], request.form['email'],
                          request.form['password'])
            return redirect(url_for('homepage'))
        else:
            cursor.close()
            error = 'This email has already been registered, Please try another!!'
    return render_template("register.html", error=error)


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('uid', None)
    flash("You were logged out!!")
    return redirect(url_for('homepage'))


if __name__ == '__main__':
    app.run()
