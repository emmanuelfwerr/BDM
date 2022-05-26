import pandas as pd
import numpy as np
import json
import glob
import os

'''
This script concatenates all individual yearly files extracted by `accidents_collector.py` and `preu_collector.py` and produces two unified .json files with all the extracted accidents information, then exports them to the persistent landing zone.
Args:
    (none)
Returns:
    (.json) accidents_opendata.json - .json file containing all extracted data opendatabcn-accidents
    (.json) preu_opendata.json - .json file containing all extracted data opendatabcn-lloguer_preu
'''
# accidents opendatabcn
path = r'landing/temporal/opendatabcn-accidents/'
all_files = glob.glob(path + "/*.csv")

li = []

for filename in all_files:
    df = pd.read_csv(filename)
    li.append(df)

df = pd.concat(li, axis=0, ignore_index=True)

df.to_json('landing/persistent/accidents_opendata/accidents_opendata.json', orient='index')

# lloguer preu opendatabcn
path = r'landing/temporal/opendatabcn-lloguer_preu/'
all_files = glob.glob(path + "/*.csv")

li = []

for filename in all_files:
    df = pd.read_csv(filename)
    li.append(df)

df = pd.concat(li, axis=0, ignore_index=True)

df.to_json('landing/persistent/preu_opendata/preu_opendata.json', orient='index')