#!/usr/bin/env python3
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys
import numpy as np
import time
import pandas as pd
from IPython.display import display
from datetime import datetime, timedelta
from sklearn.cluster import MiniBatchKMeans, KMeans
from pandarallel import pandarallel
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor

def read_mapper_output(file):
    for line in file:
        yield line

def main(separator='\t'):
    
    columns=['pickup_time', 'dropoff_time', 'trip_distance (mile)', 'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude', 'total_amount', 'trip_duration (m)', 'speed m/h']
    
    df_2015 = []
    df_2016 = []
    start_time = time.time()
    

    
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin)
    for row in data:

        key, row = row.split(separator)
        row = row.split(',')
        row[-1] = float(row[-1])
        
        if key=='2015':
            df_2015.append(row)
        elif key=='2016':
            df_2016.append(row)
        
        
        
    df_2015 = pd.DataFrame(df_2015, columns=columns)
    df_2016 = pd.DataFrame(df_2016, columns=columns)

    

    ## GEOGRAPHICAL SEGMENTATION BY CLUSTERING
    #Clustering pickups, Getting clusters 
    coord = df_2015[["pickup_latitude", "pickup_longitude"]].values
    regions = MiniBatchKMeans(n_clusters = 30, batch_size = 10000).fit(coord)
    
    #predecting clusters
    df_2015["pickup_cluster"] = regions.predict(df_2015[["pickup_latitude", "pickup_longitude"]])
    df_2016["pickup_cluster"] = regions.predict(df_2016[["pickup_latitude", "pickup_longitude"]])


    
    # Replacing mins and sec with 0
    #pandarallel.initialize() parallel_apply
    df_2015['pickup_time'] = df_2015.pickup_time.map(lambda x : pd.to_datetime(x).replace(minute=0, second=0) + timedelta(hours=1))
    df_2016['pickup_time'] = df_2016.pickup_time.map(lambda x : pd.to_datetime(x).replace(minute=0, second=0) + timedelta(hours=1))

    
    
    ## CALCULATING TAXI-DEMAND
    # Group by Cluster_id and time
    df2 = df_2015.groupby(['pickup_time','pickup_cluster']).size().reset_index(name='count')
    df1 = df_2016.groupby(['pickup_time','pickup_cluster']).size().reset_index(name='count')

    # Converting pickup counts to demand percentage
    df2['count'] = df2['count'].map(lambda x :  (x / df2['count'].max()))
    df1['count'] = df1['count'].map(lambda x :  (x / df1['count'].max()))


    
    # Getting month, days, hours, day of week
    df2['month'] = pd.DatetimeIndex(df2['pickup_time']).month
    df2['day'] = pd.DatetimeIndex(df2['pickup_time']).day
    df2['dayofweek'] = pd.DatetimeIndex(df2['pickup_time']).dayofweek
    df2['hour'] = pd.DatetimeIndex(df2['pickup_time']).hour

    df1['month'] = pd.DatetimeIndex(df1['pickup_time']).month
    df1['day'] = pd.DatetimeIndex(df1['pickup_time']).day
    df1['dayofweek'] = pd.DatetimeIndex(df1['pickup_time']).dayofweek
    df1['hour'] = pd.DatetimeIndex(df1['pickup_time']).hour
    
    
    
    # X and y for training
    X_2015_1 = df2[['pickup_cluster', 'month', 'day', 'hour', 'dayofweek']]
    y_2015_1 = df2['count']

    # X and y for testing
    X_2016_1 = df1[['pickup_cluster', 'month', 'day', 'hour', 'dayofweek']]
    y_2016_1 = df1['count']


    
    # split training data into training and validation data
    X_2015, X_2016, y_2015, y_2016 = train_test_split(X_2015_1.values, y_2015_1.values, 
                                                      test_size=0.33, random_state=42)


    # Linear regression
    LReg = LinearRegression()
    LReg.fit(X_2015, y_2015)
    LReg_y_pred = LReg.predict(X_2016_1)
    
    # RandomForest regression
    RFRegr = RandomForestRegressor()
    RFRegr.fit(X_2015, y_2015)
    RFRegr_y_pred = RFRegr.predict(X_2016_1)
    
    # XGB regression
    GBRegr = XGBRegressor(n_estimators=1000, max_depth=7, eta=0.1, subsample=0.7, colsample_bytree=0.8)
    GBRegr.fit(X_2015, y_2015)
    GBRegr_y_pred = GBRegr.predict(X_2016_1)


    
    print("--- %s seconds --- 0" % (time.time() - start_time))
    display(X_2015_1)
    display(y_2015_1)
    
    
        
if __name__ == "__main__":
    main()