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

There are many more dependencies that must be installed. You must create an environment using the provided yaml file:
- [bdm_env.yml](https://github.com/emmanuelfwerr/BDM)

### Index

- [Landing Zone](https://github.com/emmanuelfwerr/BDM/tree/main/landing)
- [Formatted Zone](https://github.com/emmanuelfwerr/BDM/tree/main/formatted)
- [Exploitation Zone](https://github.com/emmanuelfwerr/BDM/tree/main/exploitation)
- `alldata_format.py`
- `KPI_exploit.py`
- `streaming_spark.py`
- `visualize.py`

### Instructions to Run

* Download all files to your own computer while making sure to maintain file and directory hierarchy
* Must create an environment from the provided yaml file `bdm_env.yml`
* Open your terminal, go to project main directory, and run shell file `run.sh`
    * This will run each of the scripts in each zone sequentially thus completing flow of data from source to final tables

**Ignore `update_landing.sh`!** ...although it is fully operational and pretty cool, it is part of P1 of this project and unnecessary to the flow of data in P2
