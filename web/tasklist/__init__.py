# -*- coding: utf-8 -*-
import os
import pymongo
from urllib.parse import quote_plus
from flask import Flask
from flask_login import LoginManager, current_user


uri = "mongodb://{username}:{password}@{host}:{port}/{dbname}".format(username=quote_plus("root"),
                                                                      password=quote_plus("123456"),
                                                                      host=quote_plus("localhost"),
                                                                      port=27017,
                                                                      dbname="admin")
mongo_client = pymongo.MongoClient(uri)


app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev')

login_manager = LoginManager(app)


@login_manager.user_loader
def load_user(user_id):
    from tasklist.models import User
    user = User()
    user.id = user_id
    return user


login_manager.login_view = 'login'
#login_manager.login_message = '请先登录'


@app.context_processor
def inject_user():
    return dict(user=current_user)


from tasklist import views, errors
