# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import sys
import pandas as pd
from lxml import etree
import glob
import time
import pickle

#Get files to loop through
print "Reading file list. This may take a miniute"
# files = glob.glob("/Users/Jay/data/insightData/2016113*")
# files = pickle.load( open( "formtype_playlist.p", "rb" )) # faster than trying to glob on the fly
# files = pickle.load( open( "formtype_FULLlist.p", "rb" )) #
files = pickle.load( open( "formtype_lastQUARTERlist.p", "rb" )) #
print "Files to loop through %s" %len(files)
print "Setting up database"
# Database name
dbname = 'irsdb'
username = 'Jay'

## 'engine' is a connection to a database
## Here, we're using postgres, but sqlalchemy can connect to other things too.
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print engine.url

# create database
## create a database (if it doesn't exist)
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))

# connect to db
con = psycopg2.connect(database = dbname, user = username)
cur = con.cursor()

# cur.execute("DROP TABLE IF EXISTS formtype")
# cur.execute("CREATE TABLE formtype(xmlID BIGINT PRIMARY KEY, formtype VARCHAR(20), ein INTEGER)")
# con.commit()

IRS_NS = "{http://www.irs.gov/efile}" # namespace for the xml file tags

# function to grab the required info out the xml files
def getform(f):
    try:
        tree = etree.parse(f)
        root = tree.getroot()
        formtype = root.attrib.get('returnVersion')
        for elem in root.iter(tag=IRS_NS +'Filer'):
                for celem in elem.iter(tag=IRS_NS + 'EIN'):
                    ein = celem.text
        xmlID = f.strip('/Users/Jay/data/insightData/').strip('_public.xml')
        return (xmlID, formtype, ein)
    except:
        pass


# Loop through the xml files and write to the database
print "Beginning of xml loop."
con = None
con = psycopg2.connect(database = dbname, user = username)
cur = con.cursor()
t = time.time()
data = []
query = "INSERT INTO formtype (xmlID, formtype, ein) VALUES (%s, %s, %s)"
for i, f in enumerate(files):
    # if i < 20:
    try:
        data.append(getform(f))
    except:
        pass
    if i % 10000 == 0:
        try:
            cur.executemany(query, data)
            con.commit()
            data = []
            elapsed = time.time() - t
            print "file scraped : %s"  %i
            print "Time elapsed: %s" %elapsed
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e

# print "length of data %s" %len(data)
# write out remaing files
try:
    query = "INSERT INTO formtype (xmlID, formtype, ein) VALUES (%s, %s, %s)"
    cur.executemany(query, data)
    con.commit()
except psycopg2.DatabaseError, e:
    print 'Error %s' % e
finally:
    if con:
        con.close()

elapsed = time.time() - t
print "Time elapsed %s" %elapsed
