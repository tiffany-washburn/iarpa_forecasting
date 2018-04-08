#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 15:08:35 2018
@author: tiffany
"""
#Make this python 2 and 3 compliant
from __future__ import print_function

import requests
import time
import datetime
import pandas
import json
import os
from operator import itemgetter
from pathlib import Path    
from pprint import pprint #This is just to make things look pretty...

outdir = '/home/tiffany/Documents/forecasting_challenge/'
os.chdir(outdir)

import iarpa_functions as iarpa

# Pull data
secrets = {'staging':{'key':'c5ca0630222a21af9b872594ade5faea723cfad020aef6cdfb1b86f0696f5549','server':'https://api.gfc-staging.com'},
          'production':{'key':'41643c5dadbe75affec4c86ff76bdf200be6b88e09a8ef5c1d92500735d276f0','server':'https://api.iarpagfchallenge.com'}}

instance='production'
gf=iarpa.GfcApi(secrets[instance]['key'],secrets[instance]['server'],verbose=False)

# Get questions
# Once we create an instance of the GfcApi class, we retrieve Individual
# Forecasting Problems (IFPs). 
# We could limit our queries of IFPs based on date of creation or update
# (useful for finding clarifications).
# We can also limit our query to active (or closed) questions.

ifps=gf.get_questions()
print("We've downloaded {} IFPs\n".format(len(ifps)))

for ifp in ifps:
    print("IFP {}: {}".format(ifp['id'],ifp['name']))
    print("Description: {}".format(ifp['description']))
    print("Starts: {}, Ends: {}".format(ifp['starts_at'],ifp['ends_at']))
    print("Options:")
    for answer in ifp['answers']:
        print(' ({}) {}'.format(answer['id'],answer['name']))
        
    if ifp['clarifications']:
        print('Clarifications:')
        print(ifp['clarifications'])
    print("")

# Write out full listof questions
with open(outdir+'questions.json', 'w') as outfile:
    json.dump(ifps, outfile)
    
df_questions = pandas.read_json(path_or_buf = outdir+'questions.json')

# Retrieve human forecasts. 
# If we'd like, we can limit them to a particular question_id, and can
# constrain the creation or update dates.
# Let's look at an item in the human forecast stream.
# The question_id links us to the get_questions() results.
# The membership_guid is the unique identifier for a human forecaster, and will
# remain consistent throughout the GF Challenge.

# Each item in the predictions list includes the answer_id for that
# alternative, which aligns to the get_questions() output, and a
# forecasted_probability which indicates the human forecaster's submitted
# probability for that alternative.
preds_file = Path(outdir+'preds.json', 'w')
if not preds_file.is_file():
    # If no file exists, download the whole batch of predictions from the API
    preds=gf.get_human_forecasts()
    if 'errors' in preds:
        print("We ran into a problem:")
        print(preds)
    else:
        print("Retrieved {} human forecasts".format(len(preds)))
        
    # Export the predictions to a file
    with open(preds_file) as outfile:
        json.dump(preds, outfile)

# For subsequent pulls, append new data to old
# Open local file
with open(outdir+'preds.json') as infile: 
    existing_preds = json.load(infile)

# Convert to pandas dataframe
df_old_preds = pandas.read_json(path_or_buf = outdir+'preds.json')
# list(df_preds) # Get column names
max_date = max([max(df_old_preds['created_at']), max(df_old_preds['updated_at'])])
print(max_date)

# Pull new records
new_preds_created = gf.get_human_forecasts(created_after=max_date)
new_preds_updated = gf.get_human_forecasts(updated_after=max_date)

existing_preds.append(new_preds_created)
existing_preds.append(new_preds_updated)

# Read in the full data as a pandas dataframe
df_preds = pandas.read_json(path_or_buf = outdir+'preds.json')

# We can retrieve the baseline consensus forecasts using get_consensus_histories().
# As described in the API documentation, and above, after your first call to this API endpoint,
# you should constrain your requests using something like created_after while
# storing tracking older values locally.
# Note that we're using datetime.datetime() objects to specify the created and
# updated parameters. You can limit this request by question_id if desired.
# Let's look at these results. Note that each item in the list represents a
# single answer -- unlike an item in the get_human_forecasts() results where
# each entry represents the predictions for each possible answer for a single IFP.
# The normalized_value scores for all the answers to a single IFP for a specific
# consensus_at time will add up to 1.0.

# Each item in the predictions list includes the answer_id for that alternative, which aligns to the get_questions() output, and a forecasted_probability which indicates the human forecaster's submitted probability for that alternative.
cons_file = Path(outdir+'cons.json', 'w')
if not cons_file.is_file():
    # To get ALL forecasts (we don't want to do this a lot.)
    cons = gf.get_consensus_histories() 
    if 'errors' in cons:
        print("We ran into a problem:")
        print(cons)
    else:
        print("retrieved {} consensus scores".format(len(cons)))
    print(cons)
    
    # Export the predictions to a file
    with open(cons_file) as outfile:
        json.dump(cons, outfile)

# For subsequent pulls, append new data to old
# Open local file
with open(outdir+'cons.json') as infile: 
    existing_cons = json.load(infile)
    
# Convert to pandas dataframe
df_old_cons = pandas.read_json(path_or_buf = outdir+'cons.json')

# Can also read json object (list of dictionaries) and make data from
# df = pandas.DataFrame(thing)

# Get the max date any record was created or updated
max_date = max([max(df_old_cons['created_at']), max(df_old_cons['updated_at'])])
print(max_date)

# Better
# max(ifps, key=lambda x : x['created_at']) #Gets item from list
# max(x['created_at'] for x in ifps)

# Pull new records
new_cons_created = gf.get_consensus_histories(created_after=max_date)
new_cons_updated = gf.get_consensus_histories(updated_after=max_date)

existing_cons.append(new_cons_created)
existing_cons.append(new_cons_updated)

# Read in the full data as a pandas dataframe
df_cons = pandas.read_json(path_or_buf = outdir+'cons.json')
