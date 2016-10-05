from __future__ import division
from flask import render_template
from flask import request
from flaskexample import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
# import psycopg2
from b_Model import ModelIt


from cartodb import CartoDBAPIKey, CartoDBException, FileImport
import json
import io

#read in cartodb credentials from secret file
with io.open('cartodb_apikey.json') as cred:
    creds = json.load(cred)

API_KEY = creds['API_KEY']
# API_KEY = 'a251f1093712bf45ca9b4a69cd66147a2ec95320'
cartodb_domain = creds['cartodb_domain']
# cartodb_domain = 'jayfull'


cl = CartoDBAPIKey(API_KEY, cartodb_domain)

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

@app.route('/about')
def cesareans_about():
    return render_template("about.html")

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
  if patient >= 2490000: # database only make predictions up to 24900000
    patient = 24900000
  uinput = round(patient/1000000,1) #divide by 1000000 to convert into $ millions to match the database. Keep to the first deciaml point.
  uinput = str(uinput) # put dollar value (now converted to millions) into a string to match the headers in the database
  uinput = uinput.replace('.', '_')
  uinput = '_' + uinput
  try:
    print(cl.sql("UPDATE locsout SET Prob = probout." + uinput + " FROM probout WHERE probout.cbsa_short_name = locsout.cbsa_short_name"))
  except CartoDBException as e:
    print("some error ocurred", e)
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
