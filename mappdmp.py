# -*- coding: utf-8 -*-
"""
Created on Sun Apr 30 20:26:43 2017

@author: Jan Fait, jan.fait@mapp.com
"""

import os
import time
import requests
import datetime
import pandas


class InvalidCredentialsException(Exception):
    def __init__(self, message): self.message = message 

class MissingCredentialsException(Exception):
    def __init__(self, message): self.message = message

class MissingParameterException(Exception):
    def __init__(self, name): self.message = 'Missing required parameter'+name
    
class InvalidAttributeException(Exception):
    pass

class MappDmp:
   def __init__(self,root=None,username=None,password=None,debug=False):
       self.debug = debug
       self.data = {}
       self.authentication = {}
       self.session = {}
       self.constants = {}
       self.endpoints = {
       'auth':'/auth',
       'listexports':'/viz/list-exports',
       'export':'/viz/export',
       'data':'/viz/data'
       }
       self.dictionary = {
               'dimensions':['flx_interaction_timeonsite','flx_interaction_pagescroll','flx_event_type','flx_interaction_type','flx_pixel_id','flx_uuid','flx_segment_dmp','flx_conversion_dmp','flx_event_url','flx_timestamp','flx_date','flx_event_referrer_url'],
               'measures':['flx_interactions_dmp','flx_clicks_dmp','flx_impressions_dmp','flx_total_events_dmp'],
               'errors':{'export_ready':'This export is already'}
       }
       self.defaults = {
          'dimensions':['flx_event_type','flx_interaction_type','flx_pixel_id','flx_uuid','flx_segment_dmp','flx_conversion_dmp','flx_event_url','flx_timestamp','flx_date','flx_event_referrer_url'],
          'measures':['flx_interactions_dmp'],      
       }
       self
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
       self.login()
       
   def dprint(self,*args):
       if self.debug:
           args = [str(x) for x in args]      
           out = " ".join(args)
           out = "MappDmp at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': ' + out 
           print(out)
   def get_authentication(self):
       username = '='.join(('username',self.authentication['username']))
       password = '='.join(('password',self.authentication['password']))
       return '&'.join((username,password))
       
   def login(self):
       url = self.build_url('auth')
       body = self.get_authentication()
       response = requests.post(url=url,data=body)
       self.session = response.json()
       self.dprint('Loging in with:',body)
       status = self.check_login()
       return status
       
   def check_login(self):
       if self.session['response']['status'] == 'ERROR':
           self.dprint('Login failed')
           return False
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
       if not endpoint:
           return MissingParameterException
       url = ''.join((self.endpoints['root'],self.endpoints[endpoint]))
       return url
       
   def build_headers(self):
       headers = {'X-Auth': self.session['response']['token'],'X-CSRF':self.session['response']['csrf'],'Accept':'application/json'}
       return headers
       
   def call(self,endpoint=None,method='GET',params=None,body=None):
       url = self.build_url(endpoint)
       headers = self.build_headers()
       self.dprint('Calling URL',url)
       self.dprint('With headers',headers)
       
       if not self.check_login:
           self.login()
       if method == 'GET':
           response = requests.get(url=url,headers=headers)
       if method == 'POST': 
           response = requests.post(url=url,data=body,headers=headers)
           self.dprint('Sending request data',response.request.data)
       json = response.json()
       return json
       
   def list_exports(self):
       self.dprint('Fetching current export list')
       data = self.call('listexports')
       return data
       
   def get_export(self,export_id=None,stream=True):
       url = self.build_url('export')
       headers = self.build_headers()
       params = {'id':export_id}
       tempfile = 'MappDmpExport_'+ str(export_id) + '.txt'
       self.dprint('Fetching export',export_id,'to file',tempfile)
       if stream:
           r = requests.get(url,params=params,headers=headers,stream=stream)
           self.dprint('Calling URL',r.request.url)
           r.raw.decode_content = True
           with open(tempfile, 'wb') as f:
               self.dprint('Writing response content to ',tempfile)
               for chunk in r.iter_content(decode_unicode=True,chunk_size=1024):
                   if chunk:
                       f.write(chunk)
                   else:
                       f.close()
           return r.raw
       else:
          r = requests.get(url,params=params,headers=headers,stream=False)
          return r.Response.raw
    
   def get_data(self,dimensions=None,measures=None,filters=None,limit=None,batch=False,retry_period=10,add_defaults=False):
       dimensions = self.validate_dimensions(dimensions)
       measures = self.validate_measures(measures)
       filters = self.validate_filters(filters)
       query = self.prepare_query(dimensions,measures,filters,limit)
       
       if batch:
           self.dprint('Running the batch export procedure')
           response = self.call(endpoint='batch',method='POST',body=query)
           if response['status'] == 'ERROR' and response['error'] == self.dictionary['errors']['export_ready']:
               export_id = response['id']
           elif response['status'] == 'OK':
               export_ready = False
               while not export_ready:
                   time.sleep(retry_period)
                   export_ready = self.is_export_ready(export_id=export_id)
               data = self.get_export(export_id=export_id)
               return data
           else:
               return response
  
       else:
           self.dprint('Running the data query')
           response = self.call(endpoint='data',method='POST',body=query)
           return response
       
   def is_export_ready(self,export_id=None):
       self.dprint('Checking status of export',export_id)
       data = self.list_exports()
       if not data['response']['exports']:
           return False
       else:
           exports = data['response']['exports']
       for export in exports:
        status = export['state']
        self.dprint('Export',export_id,'status is',status)
        id = export['id']
        if status == 'COMPLETED' and id==export_id:
            return True
        else:
            return False
  
   def parse_input(self,input):
       if type(input) == 'str':
           output = input.split(",")
       if type(input) == 'list':
           output = input
       return output
       
   def prepare_query(self,dimensions,measures,filters,limit):
       self.dprint('Preparing query')
       query = {}
       query['dimensions'] = dimensions
       query['measures'] = measures
       query['filters'] = filters
       query['limit'] = limit
       return query
       
   def validate_measures(self,input=None):
       self.dprint('Validating measures')
       if not input:
           self.dprint('Measures not supplied, selecting default')
           return self.defaults['measures']
       else:
           input = self.parse_input(input)
           valid = self.dictionary['measures']
           output = input[input in valid]
           return output
   def validate_dimensions(self,input=None):
      self.dprint('Validating dimensions')
      if not input:
          self.dprint('Dimensions not supplied, selecting default')
          return self.defaults['dimensions']
      else:
          input = self.parse_input(input)
          valid = self.dictionary['dimensions']
          output = input[input in valid]
          return output
      
   def validate_filters(self,input):
       self.dprint('Validating filters')
       if not input:
           self.dprint('Filters not supplied, selecting default, last 1 day')
           return self.defaults['filters']
       else:
           return input
      
 


