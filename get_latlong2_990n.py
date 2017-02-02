from joblib import Parallel, delayed
import multiprocessing
import pandas as pd
import time
from pygeocoder import Geocoder
import sys
from smartystreets import Client
import json
from scipy.stats import truncnorm
import numpy as np
# import traceback

num_cores = multiprocessing.cpu_count()
client = Client('b67bca41-0b64-1782-8f67-d0954bacab04', 'ukHxCSABgbrj0gdGUiqX') #connect to SmartStreets API

df2 = pd.read_pickle('form990N_12262016_pickle') # import data frame with addresses
df2.reset_index(level=0, inplace=True) # if forget to reset index, use this line to make EIN a column, not an index
# d = dict.fromkeys(df2['EIN'])
geos = [] # empty list for lat long data

# generate lat long jitter values to add to NPOs lat long that have PO Boxes at the same post office; this wasy they all won't be overlayed on top of eachother
jitter = truncnorm(a=-0.00005/0.00005, b=0.00005/0.00005, scale=0.00005, loc=0.00005).rvs(size=10000) # generate jitter values using a truncated normal distribution. stdev = 0.00005, mean=0.00005. a and b params control the truncation. Here I am truncated at +/- 0.0005 so that I get values with a max/min of 0.00099/0.00001. Because these are small values, I should be jittering only the seconds of the latlong measurement which should be enough jitter so that all the pins aren't on top of each other but still keep the pin on the post office building.

## plot and inspect the distribution
# import matplotlib.pyplot as plt
# plt.hist(nums, bins=20)


def get_latlong(row):
    ein = row['EIN']
    try:
        address =  row['Organization Address Line 1'] + ', '+ row['Organization Address City'] + ', ' + row['Organization Address State'] + ', ' + row['Organization Address Postal Code'][0:5]
        result = client.street_address(address)
        location = result.location
        if row['Organization Address Line 1'][0:6] == 'PO BOX':
            location = tuple(location + np.random.choice(jitter,size=1,replace=True)) # randomly same from jitter values and add to lat long
        # print address, location
        return {'ein' : ein, 'latlong' : location} # return the unqiue EIN and latlong coordinates
    except AttributeError:
        # e = sys.exc_info() # another way to handle errors
        # print 'ERROR!'
        # print e
        return {'ein' : ein, 'latlong' : None}
    except TypeError:
        return {'ein' : ein, 'latlong' : None}
    except: # all other exceptions, save progress so far and then raise the error
        with open('form990N_12262016_pickle.json', 'w') as fp:
            json.dump(geos, fp)
        # e = traceback.format_exc() # another way to handle errors
        # print e
        # sys.exit()
        raise
        # try:
        #     result = Geocoder.geocode(address)
        #     location = result.coordinates
        #     print 'google', result.coordinates
        # except:
        #     e = sys.exc_info()
        #     print e
        #     try:
        #         address =  row['CITY'] + ', ' + row['STATE'] + ', ' + row['ZIP'][0:5]
        #         result = Geocoder.geocode(address)
        #         location = result.coordinates
        #         print 'google_nostreet', result.coordinates
        #     except:
        #         print address
        #         location = 'NaN'
        #         print 'NaN'


t = time.time()
geos = Parallel(n_jobs=num_cores, verbose=1)(delayed(
    get_latlong)(row)for index, row in df2.iterrows())
elapsed = time.time() - t
print elapsed

with open('eo_rest_of_usa_concat.json', 'w') as fp:
    json.dump(geos, fp)



# sudo apt-get update
# sudo apt-get install python-pip python-dev build-essential
# sudo pip install --upgrade pip
#
# sudo pip install scipy

# ssh -i ~/Dropbox/work/datasci/insight/ec2keys/ec2free.pem ubuntu@54.161.107.187
# scp -i ~/Dropbox/work/datasci/insight/ec2keys/ec2free.pem eo1_northeast ubuntu@554.161.107.187:/home/ubuntu  ##scp from local laptop to unbuntu
# scp -i ~/Dropbox/work/datasci/insight/ec2keys/ec2free.pem get_latlong2.py ubuntu@54.88.172.154:/home/ubuntu
