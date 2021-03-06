from flask import render_template
from flask import request
from flaskexample import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
# import psycopg2
from b_Model import ModelIt

# user = 'Jay' #add your username here (same as previous postgreSQL)
# host = 'localhost'
# dbname = 'birth_db'
# db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
# con = None
# con = psycopg2.connect(database = dbname, user = user)

@app.route('/')
@app.route('/index')
def cesareans_index():
    return render_template("index.html")

@app.route('/db')
def birth_page():
    sql_query = """
                SELECT * FROM birth_data_table WHERE delivery_method='Cesarean'\;
                """
    query_results = pd.read_sql_query(sql_query,con)
    births = ""
    print query_results[:10]
    for i in range(0,10):
        births += query_results.iloc[i]['birth_month']
        births += "<br>"
    return births

@app.route('/db_fancy')
def cesareans_page_fancy():
    sql_query = """
               SELECT index, attendant, birth_month FROM birth_data_table WHERE delivery_method='Cesarean';
                """
    query_results=pd.read_sql_query(sql_query,con)
    births = []
    for i in range(0,query_results.shape[0]):
        births.append(dict(index=query_results.iloc[i]['index'], attendant=query_results.iloc[i]['attendant'], birth_month=query_results.iloc[i]['birth_month']))
    return render_template('cesareans.html',births=births)

@app.route('/input')
def cesareans_input():
    return render_template("input.html")

# @app.route('/output')
# def cesareans_output():
#     return render_template("output.html")

@app.route('/output')
def cesareans_output():
  #pull 'birth_month' from input field and store it
  patient = int(request.args.get('birth_month'))

    #just select the Cesareans  from the birth dtabase for the month that the user inputs
  # query = "SELECT index, attendant, birth_month FROM birth_data_table WHERE delivery_method='Cesarean' AND birth_month='%s'" % patient
  # print query
  # query_results=pd.read_sql_query(query,con)
  # print query_results
  # births = []
  # for i in range(0,query_results.shape[0]):
  #     births.append(dict(index=query_results.iloc[i]['index'], attendant=query_results.iloc[i]['attendant'], birth_month=query_results.iloc[i]['birth_month']))
    #   the_result = ''
    #   the_result = ModelIt(patient,births)
  the_result = ModelIt(patient)
  return render_template("output.html", the_result = the_result)
