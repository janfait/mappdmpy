# -*- coding: utf-8 -*-
"""
Created on Sun Apr 30 20:26:43 2017

Thin python wrapper for the Mapp DMP API with some convenience and analytical functions

@author: Jan Fait, jan.fait@mapp.com
"""
import sys
import time
import requests
import datetime
import json
import urllib
import pandas

class MaxAttemptsReachedException(Exception):
    def __init__(self, number): self.message = 'You have reached the '+number+' attempts to retrieve an export'

class InvalidCredentialsException(Exception):
    def __init__(self, message): self.message = message 

class MissingCredentialsException(Exception):
    def __init__(self, message): self.message = message
    
class MissingParameterException(Exception):
    def __init__(self, name): self.message = 'Missing required parameter '+name
    
class InvalidAttributeException(Exception):
    pass

class MappDmp:
   def __init__(self,root=None,username=None,password=None,debug=False):
       self.debug = debug
       self.data = {}
       self.authentication = {}
       self.session = {}
       self.cache = {
           'query':{},
           'data':{},
           'request':{}
       }
       self.endpoints = {
           'auth':'/auth',
           'listexports':'/viz/list-exports',
           'export':'/viz/export',
           'data':'/viz/data',
           'batch':'/viz/batch-export',
           'names':'/report-central/search',
           'trackinglist':'/tracking/list'
       }
       self.dictionary = {
           'dimensions':['flx_advertiser_id','flx_auction_id','flx_browser','flx_buyer_id','flx_campaign_id','flx_conversion_dmp','flx_creative_id','flx_creative_size','flx_date','flx_day_in_month','flx_day_in_week','flx_day_in_year','flx_destination_url','flx_device_brand','flx_device_id_md5','flx_device_id_sha1','flx_device_id_openudid','flx_device_id_odin','flx_device_id_apple_ida','flx_device_id_google_adid','flx_device_type','flx_event_referer_url','flx_event_type','flx_event_url','flx_external_data','flx_external_pixel_id','flx_geo_city','flx_geo_country','flx_geo_lat','flx_geo_long','flx_geo_region','flx_hour','flx_insertion_order_id','flx_interaction_type','flx_interaction_value','flx_lineitem_id','flx_month_in_year','flx_operating_system','flx_pixel_id','flx_placement_id','platform','flx_platform_exchange','flx_publisher_id','flx_segment_dmp','flx_seller_id','flx_site_domain','flx_site_id','flx_site_type','flx_timestamp','flx_user_agent','flx_user_ip','flx_user_ip_truncated','flx_uuid','flx_week_in_year','flx_interaction_adhover','flx_interaction_pagescroll','flx_interaction_timeonsite'],
           'measures':['flx_clicks_dmp','flx_impressions_dmp','flx_interactions_dmp','flx_pixel_loads_dmp','flx_record_sum','flx_total_events_dmp','flx_unique_users_approx_dmp'],
           'errors':{'export_ready':'This report has already been saved to Exports due to its size. The report might be running in the background and will be available <a target="_blank" href="/batch-export">here</a>.'}
       }
       self.defaults = {
          'dimensions':['flx_event_type','flx_interaction_type','flx_pixel_id','flx_uuid','flx_segment_dmp','flx_conversion_dmp','flx_event_url','flx_timestamp','flx_date','flx_event_referer_url'],
          'measures':['flx_interactions_dmp'],
          'filters':[{'date_end': self.days_ago(0),'date_start': self.days_ago(1),'dimension': 'date'}],
          'limit':100,
          'batchlimit':10000000
       }
       self.dprint('Initializing Mapp DMP API')
       if not username or not password:
           raise MissingCredentialsException(message='username and password attributes are required')
       else:
           self.authentication['username']=username
           self.authentication['password']=password
       
       if not root:
           self.endpoints['root'] = 'https://platform.flxone.com/api'
       else:
           self.endpoints['root'] = root
       self.dprint('Initialization complete')
       
   def dprint(self,*args):
       """ Prints progress to console if class initialized with debug=True"""
       if self.debug:
           args = [str(x) for x in args]      
           out = " ".join(args)
           out = "MappDmp at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': ' + out 
           print(out)
           
   def get_authentication(self):
       """ Fetches credentials and builds an authentication string for the login function"""
       username = '='.join(('username',self.authentication['username']))
       password = '='.join(('password',self.authentication['password']))
       return '&'.join((username,password))
       
   def login(self):
       """ Performs authentication against the /api/auth endpoint with get_authentication supplying and formatting the credentials from attributes, returns bool login success"""
       url = self.build_url('auth')
       body = self.get_authentication()
       try:
           self.dprint('Loging in with:',body)
           response = requests.post(url=url,data=body)
           self.session = response.json()
           if self.session['response']['status'] == 'ERROR':
               self.dprint('Login failed with',self.session['response']['error'])
               return False
           else:
               self.dprint('Login succesfull')
               return True
       except requests.ConnectionError:
           self.dprint('Connection error occured')
           return False
 
   def check_login(self):
       """ Checks the presence of an active session, checks expiration of the token, returns True/False depending on whether a token is present and valid, returns bool login status"""
       if not 'response' in self.session:
           if not self.login():
               sys.exit('Cannot login to the Mapp DMP Platform')
       now = datetime.datetime.utcnow()
       expiry = datetime.datetime.strptime(self.session['debug']['now'], '%Y-%m-%d %H:%M:%S')
       expiry = expiry + datetime.timedelta(minutes=30)
       self.dprint('UTC timestamp is:',now)
       self.dprint('Token expires at:',expiry,'UTC')
       if now>=expiry:
           self.dprint('Token expired')
           return False
       else:
           self.dprint('Token valid')
           return True
           
   def build_url(self,endpoint=None):
       """ Joins the root endpoint and the endpoint selected by the call function to create the base URL, returns URL as a string"""
       if not endpoint:
           return MissingParameterException
       url = ''.join((self.endpoints['root'],self.endpoints[endpoint]))
       return url
       
   def build_headers(self):
       """ Collects the token and CSRF tokens from the active session and supplies them with other headers to the call function"""
       headers = {'X-Auth': self.session['response']['token'],'X-CSRF':self.session['response']['csrf'],'Accept':'application/json'}
       return headers
       
   def call(self,endpoint=None,method='GET',params=None,body=None):
       """
       Calls the endpoints, differentiating between GET/POST methods, 
       appends headers, parameters and request body, 
       listens for ConnectionError. 
       
       Args:
           endpoint -- str name of the endpoint (default None)
           imag -- str HTTP method (default 'GET')
       Returns:
           requests Response content
       """
       
       if not self.check_login():
               sys.exit('Cannot login to the Mapp DMP Platform')
       url = self.build_url(endpoint)
       headers = self.build_headers()
       self.dprint('Calling URL',url)
       self.dprint('With headers',headers)
       if method == 'GET':
           try:
               response = requests.get(url=url,headers=headers)
               data = response.json()
               return data
           except ConnectionError:
               self.dprint('Connection error occured')
               return False 
       if method == 'POST':
           try:
               self.dprint('Sending request data',body)
               response = requests.post(url=url,data=body,headers=headers)
               data = response.json()
               return data
           except ConnectionError:
               self.dprint('Connection error occured')
               return False 
           
   def list_exports(self):
       """ Calls the /list-exports endpoint, returns a JSON string"""
       self.dprint('Fetching current export list')
       data = self.call('listexports')
       return data
       
   def get_export(self,export_id=None,chunk_size=1024, target_filename=None,return_content=True):
       """
       Calls the /export URL and streams the export to file  
       
       Args:
           export_id -- num ID returned by the get_data(batch=True) method (default None)
           chunk_size -- num Byte size of the chunks streamed from the response (default 1024)
           filename -- str filename to which the export will be saved (default MappDmpExport_EXPORT_ID_YYYY_MM_DD.txt)
           
       Returns:
           pandas DataFrame
       """
       if not self.check_login():
           if not self.login():
               sys.exit('Cannot login to the Mapp DMP Platform')
       if not export_id:
           raise MissingParameterException('export_id')
       url = self.build_url('export')
       headers = self.build_headers()
       params = {'id':export_id}
       if not target_filename:
           tempfile = 'MappDmpExport_'+ str(export_id) + '_'+ self.days_ago(0)+ '.txt'
       else:
           tempfile = target_filename
       self.dprint('Fetching export',export_id,'to file',tempfile)
       r = requests.get(url,params=params,headers=headers,stream=True)
       self.dprint('Calling URL',r.request.url)
       r.raw.decode_content = True
       with open(tempfile, 'wb') as f:
           self.dprint('Streaming response content to ',tempfile)
           for chunk in r.iter_content(decode_unicode=True,chunk_size=chunk_size):
               if chunk:
                   f.write(chunk)
               else:
                   f.close()
       if return_content:
           data = pandas.read_csv(tempfile,compression='gzip')
       else:
           data = tempfile
       return data

   def get_data(self,dimensions=None,measures=None,filters=None,limit=None,batch=False,retry_period=10,max_attempts=30,add_defaults=False):
       """
       Calls the /export URL and streams the export to file  
       
       Args:
           dimensions -- list of string names of identifiers offered by the Mapp DMP API (default defined by self.defaults['dimensions'])
           measures -- list of string names of identifiers offered by the Mapp DMP API (default defined by self.defaults['measures'])
           filters -- list of dictionary  (default defined by self.defaults['filters'])
           limit -- num number of records to return default(if batch=True, 1000000, batch=False 5000)
           batch -- bool if True, calls the /batch-export endpoint, if False, calls the /data endpoint (default False)
           retry_period -- num number of seconds after which the export status will be rechecked (default 10)
           max_attempts -- num maximum number of attempts to retrieve the export (default 30)
           add_defaults -- bool if True, adds all default dimensions and measures to the user supplied (default False)
       Returns:
           pandas DataFrame
           
       """
       query = self.prepare_query(dimensions,measures,filters,limit,batch)
       if batch:
           self.dprint('Running the batch export procedure')
           response = self.call(endpoint='batch',method='POST',body=query)
           if response['response']['status'] == 'ERROR' and response['response']['error'] == self.dictionary['errors']['export_ready']:
               export_id = response['response']['id']
               self.dprint('Export',export_id,'has been completed previously')
               data = self.get_export(export_id=export_id)
               return data
           elif response['response']['status'] == 'OK':
               export_id = response['response']['id']
               export_ready = False
               attempt_counter = 0
               if not retry_period:
                   return export_id
               while not export_ready:
                   time.sleep(retry_period)
                   self.dprint('Attempt number',attempt_counter,', export is ready:',export_ready)
                   export_ready = self.is_export_ready(export_id=export_id)
                   attempt_counter += 1
                   if attempt_counter>max_attempts:
                       raise MaxAttemptsReachedException(max_attempts)
                       break
               data = self.get_export(export_id=export_id)
               return data
           else:
               return response
  
       else:
           self.dprint('Running the data query')
           response = self.call(endpoint='data',method='POST',body=query)
           if 'data' in response['response']:
               data = response['response']['data']
               if not data:
                   return response['response']
               data = json.dumps(data[0]['data'])
               data = pandas.read_json(data)
               return data
           else:
               return response
   
   def is_export_ready(self,export_id=None):
       """ Retrieve all pixels defined in the Mapp DMP instance"""
       if not export_id:
           raise MissingParameterException('export_id')
       self.dprint('Checking status of export',export_id)
       data = self.list_exports()
       if not data['response']['exports']:
           self.dprint('Exports not found in response')
           return False
       else:
           exports = data['response']['exports']
       ready = False
       for export in exports:
           xid = export['id']
           status = export['state']
           self.dprint('Export',xid,'status is',status)
           if status == 'COMPLETED' and str(xid)==str(export_id):
               self.dprint('Returning True')
               ready = True
               break
       return ready
       
   def get_pixels(self):
       """ Retrieve all pixels defined in the Mapp DMP instance"""
       response = self.call(enpoint="trackinglist")
       if 'beacons' in response['response']:
           data = response['beacons']['methods']
           return data
       else:
           return response

   def parse_input(self,data):
       """ Parses comma separated string into a list"""
       if type(data) == str:
           out = data.split(",")
           return out
       if type(data) == list:
           out = data
           return out
   
   def days_ago(self,days=1):
       """ Returns a string date X days ago in YYYY-MM-DD format, where X is defined by the days parameter"""
       out = datetime.datetime.now() - datetime.timedelta(days=days)
       out = out.strftime("%Y-%m-%d")
       return out
       
   def prepare_query(self,dimensions=None,measures=None,filters=None,limit=None,batch=False):
       """ Prepares the query object to be supplied to the get_data() method, including URL encoding and JSON formatting, returns a URL encoded JSON formated query"""
       self.dprint('Preparing query')
       dimensions = self.validate_dimensions(dimensions)
       measures = self.validate_measures(measures)
       filters = self.validate_filters(filters)
       if not limit:
           if batch:
               limit = self.defaults['limit']
           else:
               limit = self.defaults['batchlimit']
       
       query = {}
       query['dimensions'] = dimensions
       query['measures'] = measures
       query['filters'] = filters
       query['limit'] = limit
       query = [query]
       self.dprint('Raw query',query)
       query = urllib.parse.quote_plus(json.dumps(query))
       query = "x="+query
       return query
   
   def check_prefix(self,data):
       """Prepends 'flx_' to all input if prefix missing"""
       data = ['flx_'+d if d[:4]!='flx_' else d for d in data]
       return data
       
   def validate_measures(self,data=None):
       """ Validates user supplied measures against the self.dictionary['measures'] and removes all that are not allowedm, if not provided, returns defaults"""
       self.dprint('Validating measures')
       if not data:
           self.dprint('Measures not supplied, selecting default')
           return self.defaults['measures']
       elif data == "*":
           out = self.dictionary['dimensions']
           return out  
       else:
           data = self.parse_input(data)
           data = self.check_prefix(data)
           valid = self.dictionary['measures']
           out = list(set(valid).intersection(set(data)))
           return out
       
   def validate_dimensions(self,data=None):
       """ Validates user supplied dimensions against the self.dictionary['dimensions'] and removes all that are not allowed, if not provided, returns defaults"""
       self.dprint('Validating dimensions')
       if not data:
           self.dprint('Dimensions not supplied, selecting default')
           return self.defaults['dimensions']
       elif data == "*":
           out = self.dictionary['dimensions']
           return out  
       else:
          data = self.parse_input(data)
          data = self.check_prefix(data)
          valid = self.dictionary['dimensions']
          out = list(set(valid).intersection(set(data)))
          return out
      
   def validate_filters(self,data=None):
       """Validates that filters include the date dimension, if not provided, returns defaults"""
       self.dprint('Validating filters')
       if not data:
           self.dprint('Filters not supplied, selecting default, last 1 day')
           return self.defaults['filters']
       else:
           return data
      
 


