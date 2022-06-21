# BDM P2
This repository showcases the Final BDM project. Where we are tasked with Integrating and Reconciling several datasets from Barcelona OpenData with data from Idealista apartment listings. We clean and process the data in order to support Descriptive Data Analytics of historical data as well as Predictive Data Analytics of real-time data using Spark Streaming.

Below are the project and environment details as well as instructions to run the project on your machine!

### Environment

This project requires **Python 3.9** and the following Python libraries installed:

- [pandas](https://pandas.pydata.org/docs/)
- [numpy](https://numpy.org/doc/)
- [seaborn](https://seaborn.pydata.org)
- [pyspark](https://spark.apache.org/docs/latest/api/python/)
- [dash](https://dash.plotly.com)
- [plotly](https://dash.plotly.com)

There are many more dependencies that must be installed. You must create an environment using the provided yaml file `bdm_env.yml`

### Index

- [landing](https://github.com/emmanuelfwerr/BDM/tree/main/landing) - Landing Zone Directory where P1 data resides (also in mongoDB)
- [formatted](https://github.com/emmanuelfwerr/BDM/tree/main/formatted) - Formatted Zone Directory where P2 data resides (also in mongoDB)
- [exploitation](https://github.com/emmanuelfwerr/BDM/tree/main/exploitation) - Exploitation Zone Directory where P2 model resides
- `alldata_format.py` - Preprocess all data from Persistent Landing and send to Formatted Zone
- `KPI_exploit.py` - Import all data from Formatted Zone and create KPI tables, then upload tables to Exploitation Zone
- `streaming_spark.py` - Connect to Kafka stream and apply predictive model to incoming messages in real-time
- `visualize.py` - Import all data from Exploitation Zone and create simple KPI dashboard in localhost using Dash and Plotly
- `test_visuals.ipynb` - Testing KPI visualizations in Jupyter Notebook
- `run.sh` - shell script to run all P2 files in correct sequence and produce KPI hasboard in localhost
- `opendata_collect.py` - Check if Temporal Landing is up to date... if not, collect necessary data from OpenDataBCN (P1)
- `opendata_persist.py` - Export all OpenDataBCN data from Temporal Landining to Persistent Landing (P1)
- `update_landing.sh` - shell script to run all P1 files in correct sequence
- `bdm_env.yml` - environment yaml file

### Instructions to Run

* Download all files to your own computer while making sure to maintain file and directory hierarchy
* Must create an environment from the provided yaml file `bdm_env.yml`
* Open your terminal, go to project main directory, and run shell file `run.sh`
    * This will run each of the scripts in each zone sequentially thus completing flow of data from source to final tables
* `visualize.py` will remain running until Keyboard Interrupt
    * Access http://127.0.0.1:8050/ from your browser to view KPI dashboard

**Ignore `update_landing.sh`!** ...although it is fully operational and pretty cool, it is part of P1 of this project and unnecessary to run in P2. Feel free to take a look inside though!
