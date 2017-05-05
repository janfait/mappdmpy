# -*- coding: utf-8 -*-
"""
Created on Wed May  3 08:52:54 2017

@author: Jan.Fait
"""

#define where your folder is located
mappdmp_loc = "C:/Users/jan.fait/Documents/WinPython/dev/mappdmp"
#your Mapp DMP username
mappdmp_username = "jan.fait@mapp.com"
#your Mapp DMP password, consider using hashing and/or database storage of your passwords
mappdmp_password = "xxxxxxxxx"
#define the pixel you can supply as a filter (optional), API retrieves all pixel data if you don't
my_pixel = 12345

#import of the libraries
import sys
#append your path if you are not operating from the same directory
sys.path.append(mappdmp_loc)
#import the mappdmp module
import mappdmp

#initialize an instance, keep debug True if you want to see what is happening behind the scenes
my_dmp = mappdmp.MappDmp(username=mappdmp_username,password=mappdmp_password,debug=True)

#define some dimensions as a python list
my_dimensions = ['flx_uuid','flx_event_type','flx_date']
#define some measures as a python list
my_measures = ['flx_interactions_dmp','flx_unique_users_dmp']
#define some filters as a list of dictionary, supply your variables dynamically. If you omit the dimension:date node, you will get data from the last day
my_filters = [{'dimension':'pixel_id','includes':my_pixel},[{'dimension': 'date','date_end': '2017-05-05','date_start':'2017-05-03'}]]

#this particular query will return a pandas DataFrame with 5 columns and 100 rows, provided the filter input was correctly specified
my_data = my_dmp.get_data(
    dimensions = my_dimensions,
    measures = my_measures,
    filters = my_filters,
    limit = 100,
    batch = False
)

print(my_data)

#this is the exact same query, only with no row limit passed to Mapp DMP API as a batch-export request. 
#This will start the procedure of checking of export status and ultimately return the pandas DataFrame
#from the file the export was streamed to
my_batch_data = my_dmp.get_data(
    dimensions = my_dimensions,
    measures = my_measures,
    filters = my_filters,
    limit = None,
    batch = True
)
