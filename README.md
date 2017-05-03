# mappdmpy

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

import sys
sys.path.append("/wherever/your/folder/is/mappdmp")

### Initialize
All you need really are the login credentials. To understand what the package is doing in the background, you can initialize it 
with debug=True parameter. 

import mappdmp
my_dmp = mappdmp.MappDmp(username='yourusername',password='yourpassword',debug=T)

### Login
The login function is implicit. This means that whenever the module calls the Mapp DMP API, it will check the existing session and if the token is not present or expired, it will trigger the login() function. If however you want to test your login credentials, you can just do:

my_dmp.login()



