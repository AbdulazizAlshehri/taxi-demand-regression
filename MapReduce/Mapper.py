#!/usr/bin/env python3

import sys
from itertools import islice
from operator import itemgetter
import pandas as pd
from datetime import datetime
import time


def read_input(file):
    # read file except first line
    for line in islice(file, 1, None):
        # split the line into words
        yield line.split(',')

def main(separator='\t'):


    # NYC coordinates boundaries
    min_long = -74.25559136315209
    max_long = -73.7000090639354
    min_lat = 40.496115395170364
    max_lat = 40.91553277700258

    # pickup_duration in minutes
    min_trip_dur = 0
    max_trip_dur = 720

    # trip_distance in miles
    min_trip_dis = 0
    max_trip_dis = 77.5

    # speed in mile/hour
    min_speed = 0
    max_speed = 63

    #
    line = []

    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    
    # needed columns
    # tpep_dropoff_datetime, trip_distance, dropoff_longitude, dropoff_latitude, total_amount
    #  , tpep_pickup_datetime, pickup_latitude, pickup_longitude


    #
    for row in data:

        # for each row we take only the needed columns
        row = list(itemgetter(*[1, 2, 4, 5, 6, 9, 10, 18])(row))
        #row[7] = row[7].replace('\n', '')
        row[-1] = float(row[-1])



        # 1. CLEAN, any row has NaN 


        # 2. CLEAN, pickup_longitude
        row[3] = float(row[3])
        if row[3] < min_long or row[3] > max_long:
            #print(bcolors.WARNING + '%s%s' % ('WRONG longitude! ', row) + bcolors.ENDC)
            continue

        # 3. CLEAN, pickup_latitude
        row[4] = float(row[4])
        if row[4] < min_lat or row[4] > max_lat:
            #print(bcolors.WARNING + '%s%s' % ('WRONG latitude! ', row) + bcolors.ENDC)
            continue

        # 4. COMPUTE & CLEAN, trip_duration
        t1 = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        t2 = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
        row.append(round((t2 - t1).seconds/60, 3))
        if row[-1] <= min_trip_dur or row[-1] > max_trip_dur:
            #print(bcolors.WARNING + '%s%s' % ('WRONG duration! ', row) + bcolors.ENDC)
            continue

        # 5. CLEAN trip_distance
        row[2] = float(row[2])
        if row[2] <= min_trip_dis or row[2] > max_trip_dis:
            #print(bcolors.WARNING + '%s%s' % ('WRONG distance! ', row) + bcolors.ENDC)
            continue

        # 6. COMPUTE & CLEAN speed miles/hour = (trip_distance in miles / trip_duration in minutes / 60)
        row.append(round(row[2]/row[-1]*60, 3))
        if row[-1] <= min_speed or row[-1] > max_speed:
            #print(bcolors.WARNING + '%s%s' % ('WRONG speed! ', row) + bcolors.ENDC)
            continue

        # 7. discard unneeded columns
        #    we keep pickup_time, pickup_longitude and pickup_latitude
        row = [row[0], row[3], row[4]]

        
        
        # print here will send the output to reducer
        # for now this line consedring training data can be Only from 2015 and test data can be from any year
        key = 'train' if row[0].split('-')[0] == '2015' else 'test'
        val = ','.join(str(v) for v in row)
        pair = '%s%s%s' % (key, separator, val)
        print(pair)
        
    
    
if __name__ == "__main__":
    main()