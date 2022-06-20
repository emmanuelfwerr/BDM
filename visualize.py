# Run this app with `python visualize.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd

import findspark
findspark.init()

import pyspark
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType


def loadMongoDF(db, collection):
    '''
    Download data from mongodb and store it in DF format
    '''
    spark = SparkSession \
        .builder \
        .master(f"local[*]") \
        .appName("myApp") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    dataDF = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/{db}.{collection}") \
        .load()

    return dataDF, spark

kpi1DF, spark = loadMongoDF(db='exploitation', collection='kpi1')
        
df = kpi1DF.toPandas()

app = Dash(__name__)

app.layout = html.Div([
    #html.H4(children=''), # Some text
    
    # Correlation variable in x-axis
    html.Div([
            "Select first variable to plot (x-axis):",
            dcc.Dropdown(
            # Only show numeric columns
                df._get_numeric_data().columns.values.tolist(),
                df._get_numeric_data().columns.values.tolist()[1],
                id='xaxis-column'
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        
    # Correlation variable in y-axis
    html.Div([
            "Select second variable to plot (y-axis):",
            dcc.Dropdown(
            # Only show numeric columns
                df._get_numeric_data().columns.values.tolist(),
                df._get_numeric_data().columns.values.tolist()[1],
                id='yaxis-column'
            )
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}),
    
    # Correlation
    html.Div(id="number-output"),
    
    # Graph
    dcc.Graph(id='indicator-graphic')
])

@app.callback(
    Output('indicator-graphic', 'figure'),
    Input('xaxis-column', 'value'),
    Input('yaxis-column', 'value'))

    
def update_graph(xaxis_column_name, yaxis_column_name):
    #dff = df[df['Year'] == year_value]

    fig = px.scatter(x=df.loc[:,xaxis_column_name],
                     y=df.loc[:,yaxis_column_name],
                     title = u'Correlation between {} and {} is: {}'.format(xaxis_column_name,
                             yaxis_column_name, round(df[xaxis_column_name].corr(df[yaxis_column_name]), 3)),
                     width=800, height=500
                     #hover_name=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name']
                     )

    fig.update_layout(margin={'l': 40, 'b': 40, 't': 40, 'r': 40}, hovermode='closest')

    fig.update_xaxes(title=xaxis_column_name)
    fig.update_yaxes(title=yaxis_column_name)

    return fig
    

if __name__ == '__main__':
    app.run_server(debug=True)