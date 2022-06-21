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

kpi2DF, spark = loadMongoDF(db='exploitation', collection='kpi2')
        
kpi2_df = kpi2DF.toPandas()
kpi1_df = kpi1DF.toPandas()
kpi1_df.sort_values('average_price', ascending=True, inplace=True)

app = Dash(__name__)

kpi1_fig = px.bar(kpi1_df, x="neighborhood", y="average_price", text_auto='.2s', title="Average Listing Price per Neighborhood")
kpi1_fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
kpi1_fig.update_layout(barmode='group', xaxis_tickangle=-45)

kpi1_fig2 = px.bar(kpi1_df, x="neighborhood", y="n_listings", text_auto='.2s', title="Total Available Listings per Neighborhood")
kpi1_fig2.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
kpi1_fig2.update_layout(barmode='group', xaxis_tickangle=-45)

app.layout = html.Div([
    #html.H4(children=''), # Some text
    
    # Correlation variable in x-axis
    html.Div([
            "Select first variable to plot (x-axis):",
            dcc.Dropdown(
            # Only show numeric columns
                kpi2_df._get_numeric_data().columns.values.tolist(),
                kpi2_df._get_numeric_data().columns.values.tolist()[0],
                id='xaxis-column'
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        
    # Correlation variable in y-axis
    html.Div([
            "Select second variable to plot (y-axis):",
            dcc.Dropdown(
            # Only show numeric columns
                kpi2_df._get_numeric_data().columns.values.tolist(),
                kpi2_df._get_numeric_data().columns.values.tolist()[1],
                id='yaxis-column'
            )
        ], style={'width': '48%', 'float': 'right', 'display': 'inline-block'}),
    
    # Correlation
    html.Div(id="number-output"),
    
    # kpi2 Graph
    dcc.Graph(id='indicator-graphic'),

    # kpi1 Graph
    dcc.Graph(
        id='kpi1_price-graph',
        figure=kpi1_fig),

    # kpi1 Graph2
    dcc.Graph(
        id='kpi1_listings-graph',
        figure=kpi1_fig2)
])

@app.callback(
    Output('indicator-graphic', 'figure'),
    Input('xaxis-column', 'value'),
    Input('yaxis-column', 'value'))

    
def update_graph(xaxis_column_name, yaxis_column_name):
    #dff = df[df['Year'] == year_value]

    fig = px.scatter(x=kpi2_df.loc[:,xaxis_column_name],
                     y=kpi2_df.loc[:,yaxis_column_name],
                     title = u'Correlation between {} and {} is: {}'.format(xaxis_column_name,
                             yaxis_column_name, round(kpi2_df[xaxis_column_name].corr(kpi2_df[yaxis_column_name]), 3)),
                     width=800, height=500
                     #hover_name=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name']
                     )

    fig.update_layout(margin={'l': 40, 'b': 40, 't': 40, 'r': 40}, hovermode='closest')

    fig.update_xaxes(title=xaxis_column_name)
    fig.update_yaxes(title=yaxis_column_name)

    return fig
    

if __name__ == '__main__':
    app.run_server(debug=True)