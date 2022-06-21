#! /bin/bash

BASEDIR=./

# running alldata_format script
echo "Cleaning and Moving all data from Persistent Zone to Formatted Zone"
python alldata_format.py

# running KPI_exploit script
echo "Computing KPIs and Training Predictive Model"
python KPI_exploit.py

# running spark_streaming script
echo "Applying Predictive Model to Incoming Spark Streaming Messages"
python streaming_spark.py

# running visualize script
echo "Visualizing Descriptive KPIs with Dash... running on http://127.0.0.1:8050/"
python visualize.py