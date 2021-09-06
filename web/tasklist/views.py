# -*- coding: utf-8 -*-
from flask import render_template, request, url_for, redirect, flash
from flask_login import login_user, login_required, logout_user, current_user

from tasklist import app, mongo_client
from tasklist.models import User


@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    tasks = []
    researcher_db = mongo_client[current_user.id]
    try:
        tasks = [r for r in researcher_db.task.find()] #读取task表里面所有记录
    except Exception as err:
        print(err)
    return render_template('index.html', tasks=tasks)


@app.route('/task/delete/<string:task_id>', methods=['POST'])
@login_required
def delete(task_id):
    from bson.objectid import ObjectId
    researcher_db = mongo_client[current_user.id]
    researcher_db.task.delete_one({ "_id": ObjectId(task_id) })
    flash('Item deleted.')
    return redirect(url_for('index'))


@app.route('/task/stop/<string:task_id>', methods=['POST'])
@login_required
def stop(task_id):
    flash('Item stoped.')
    return redirect(url_for('index'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if not username or not password:
            flash('Invalid input.')
            return redirect(url_for('login'))
        researcher_db = mongo_client[username]
        try:
            if researcher_db.authenticate(name=username, password=password, source=username):
                user = User()
                user.id = username
                login_user(user)
                flash('Login success.')
                return redirect(url_for('index'))
        except Exception as e:
            print(e)
        flash('Invalid username or password.')
        return redirect(url_for('login'))
    return render_template('login.html')


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Goodbye.')
    return redirect(url_for('login'))
