# mappdmpy

!!!! This module is under development, all production use at your own risk !!!!

Thin python wrapper for the Mapp DMP API with some convenience and analytical functions

## Author
Jan Fait, jan.fait@mapp.com
Marketing Analytics at Mapp

## About
This is an unofficial project to make manipulation with raw data exports from the Mapp Data Management Platform somewhat easier for
analysts and data scientists. The early release will just wrap the API, later analytical functions will be added.

## Getting started


### Setup
Depending on the stage of development, you may need to add the folder to your path like
```python
import sys
sys.path.append("/wherever/your/folder/is/mappdmp")
```
### Initialize
All you need really are the login credentials. To understand what the package is doing in the background, you can initialize it 
with debug=True parameter. 
```python
import mappdmp
my_dmp = mappdmp.MappDmp(username='yourusername',password='yourpassword',debug=T)
```
### Login
The login function is implicit. This means that whenever the module calls the Mapp DMP API, it will check the existing session and if the token is not present or expired, it will trigger the login() function. If however you want to test your login credentials, you can just do:

```python
my_dmp.login()
#returns True or False
```

## Getting Data

The core function of the module is the get_data() function.
You can supply your dimensions,measures,filters and limit to it just like you would in the JSON body of the MAPP DMP API request. If you don't the module will supply defaults for each parameter.
You can review and even redefine defaults like:

```python

defaults = my_dmp.defaults
print(defaults)

#... do something with the defaults

my_dmp.defaults = new_defaults

```

Just like the Mapp DMP API, the module offers two ways to grab the raw data. You are free to choose which one to use, but note there is no intelligent mechanism that determines the size of the reponse and chooses the method accordingly. The method is determined in the batch=True/False parameter supplied to the get_data() function. 

### 1. batch=False queries the /viz/data endpoint


```python

#returns a json formatted response
my_data = my_dmp.get_data(
    dimensions=['flx_uuid','flx_date','flx_timestamp','flx_event_type'],
    measures=['flx_interactions_dmp'],
    filters= [{'date_end': '2017-05-02','date_start': '2017-05-01','dimension': 'date'},{'dimension': 'pixel_id', 'includes': '10000'}],
    limit = 500,
    batch = False
)
```

### 2. batch=True queries the /viz/batch-export/ endpoint

A request to the batch-export endpoint is much more complicated that the simple immediate export. The module submits the export request and continues to check its status by querying the viz/list-exports endpoint periodically until it has been completed.
You can specify the period by the retry_period parameter. Also, if you set retry_period to None, the get_data function will only perform the request and only deliver the export id which you may later use in the .get_export(export_id=YOUR_EXPORT_ID) function.

This will request the export, wait for its execution and once ready, stream the content of the export into a binary file named like 'MappDmpExport_YOUR_EXPORT_ID' in the current working directory.

```python
#requests the export and checks every 10 seconds whether it has been completed
my_data = my_dmp.get_data(
    dimensions=['flx_uuid','flx_date','flx_timestamp','flx_event_type'],
    measures=['flx_interactions_dmp'],
    filters= [{'date_end': '2017-05-02','date_start': '2017-04-01','dimension': 'date'},{'dimension': 'pixel_id', 'includes': '10000'}],
    limit = 500,
    retry_period=10,
    batch = True
)
```

This will only return the YOUR_EXPORT_ID which you then later, whenever you need it, request with the .get_export() method. You can check the status of the export by calling the .is_export_ready(export_id=YOUR_EXPORT_ID) method.

```python
#requests the export and checks every 10 seconds whether it has been completed
my_data = my_dmp.get_data(
    dimensions=['flx_uuid','flx_date','flx_timestamp','flx_event_type'],
    measures=['flx_interactions_dmp'],
    filters= [{'date_end': '2017-05-02','date_start': '2017-04-01','dimension': 'date'},{'dimension': 'pixel_id', 'includes': '10000'}],
    limit = 500,
    retry_period=None,
    batch = True
)

#requesting the export content with an additional check
if is_export_ready(export_id=YOUR_EXPORT_ID):
    my_dmp.get_export(export_id=YOUR_EXPORT_ID)
else:
    print("Export is not ready yet")

```
