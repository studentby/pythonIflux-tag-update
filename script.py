import json  
import argparse
from os import name, write
from time import time
from typing import Protocol
from influxdb import InfluxDBClient
import statistics

from datetime import date, datetime
from time import mktime



## Gathering info from JSON config file:

# Take arguments from terminal

parser = argparse.ArgumentParser()

parser.add_argument("--config", help="config file absolute path",default="./config.json")
parser.add_argument("--tag_key","--k", action='append',dest='tag_key_list', help="tag key")
parser.add_argument("--tag_value","--v" ,action='append',dest='tag_value_list' ,help="tag value")

## Optional argument to delete with given tags

delete_group = parser.add_argument_group('delete_group')
delete_group.add_argument('--delete', action='store_true',default=False)

## Optional tag to add in existing Series of measurements

update_group = parser.add_argument_group('update_group')
update_group.add_argument('--update', action='store_true',default=False)
update_group.add_argument('--insert_key','--ik', action='append', help="update key")
update_group.add_argument('--insert_value','--iv', action='append', help="update value")



test_group = parser.add_argument_group('test_group')
test_group.add_argument('--test',action='store_true',default=False)

prod_group = parser.add_argument_group('prod_group')
prod_group.add_argument('--prod',action='store_true',default=False)
args = parser.parse_args()
## Extracting script configuration

# file_Path taken from terminal 

file_Path = args.config
file = open(file_Path,"r")
cred = json.loads(file.read())
file.close()

# Reading from JSON config END

# Taking values from exact position

userName=cred["influxdb"]["username"]
userPassword=cred["influxdb"]["password"]
hostName=cred["influxdb"]["host"]
portNum=cred["influxdb"]["port"]
DB_name=cred["influxdb"]["database"]
ssl_connection=cred["influxdb"]["SSL"]
verify_ssl=cred["influxdb"]["verify_ssl"]

## Entering Data base with values from config

# Localy checked connection to DB 
# client = InfluxDBClient(host=hostName, port=portNum, username=userName , password=userPassword, database=DB_name)

# With SSL conection: 
client = InfluxDBClient(host=hostName, port=portNum, username=userName, password=userPassword, ssl=ssl_connection, verify_ssl=verify_ssl)

database_list = client.get_list_database()

# For user to switch beetween databases, taken from JSON config

client.switch_database(DB_name)

## Functions 

# Gather user query

def query(key,value):
    request_list = []
    for i in range(len(key)):
        request_list.append(f"\"{key[i]}\"" + "=" + f"'{value[i]}'")
    join_query = ' AND '.join(request_list)
    return join_query

# Gather query for summary results

def query_tags(key,value):
    request_list_tags = []
    for i in range(len(key)):
        request_list_tags.append(f"{key[i]}={value[i]}")
    join_query_tags = ' AND '.join(request_list_tags)
    return join_query_tags
# Deletion function

def tags_delete(TARGkey,TARGval):
    join_query = query(TARGkey,TARGval)
    # Show Series to check
    if args.test == True:
        print(client.query(f"SHOW SERIES WHERE {join_query}"))
    elif args.prod ==True:
        # Drop data series:
        (client.query(f"DROP SERIES WHERE {join_query}"))
        print("Droping series executed")
        
    else: print("Choose between --prod --test to execute or check ")

## Update function

def update_tags(TARGkey,TARGval, update_tag_key, update_tag_value):
    join_query = query(TARGkey,TARGval)
    join_query_tags = query_tags(TARGkey,TARGval)
    ## Exrtacting time from measurenment
    rs_time = client.query(f'SELECT median FROM "SpeedIndex" WHERE {join_query}')
    rs_time_list = list(rs_time.get_points())
    ## time extraction
    time_field = []
    for time in dict (rs_time_list[0]).values():
        time_field.append(time)
    time_value = time_field[0]
    
    ## time to UNIX (EPOCH) conversion
    date_time = datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%S.%fZ")
    unix_time = date_time.timestamp() * 1000000000
    print(int(unix_time))
   
    ## Approach for Summary results only
    dictionaery_median = dict()
    dictionaery_page_median = dict()
    median_list_values = []
    ## Summary measurements
    measurement_list = ['FirstVisualChange','first-contentful-paint','renderTime','VisualComplete85','SpeedIndex','layoutShift']
    for measure in range(len(measurement_list)):
        tag_set_list = []
        write_data_list = []
        pageSummary_median_list = []

        # Gathering other tag sets
        rs_tags_extract = client.query(f'SHOW TAG KEYS FROM "{measurement_list[measure]}"')
        tags_extract = list(rs_tags_extract.get_points())
        
        for tags_index in tags_extract:
            for value in tags_index.values():
                rs_tag_values = client.query(f'SHOW TAG VALUES FROM "{measurement_list[measure]}" WITH KEY="{value}" WHERE {join_query}')
                tag_values_list = list(rs_tag_values.get_points())
            
            if len(tag_values_list) < 2:
                    
                index_eql = 0
                if tag_values_list == []:
                    pass
                else:
                    for tagSetLength in range(len(tag_values_list)):
                        for Key,tagSet in dict(tag_values_list[tagSetLength]).items():
                            tag_set_list.append( tagSet )
                            index_eql = index_eql+1                        
                            if index_eql == 1:
                                tag_set_list.append('=')
                if index_eql == 2:
                    tag_set_list.append(',')
            ## Do not add tags to list of tags, bcs they fail as "duplicated tags" errors
            else: pass    
        joined_tag_set_list = ''.join(tag_set_list)
        # there are VisualComplete85 SpeedIndex FirstVisualChange metrics have summary results, others needs to be measured medians of pages tested

        median_values = client.query(f'SELECT median FROM "{measurement_list[measure]}" WHERE "summaryType"=\'pageSummary\' AND {join_query}')
        median_list = list(median_values.get_points())
        
        # query to extract requeried medians and remove time field (included i as index to filter out)
        i = 0
        median_list_values = []
        if median_list == []:
            # largestContentfulPaint requres speciall treatment

            if measurement_list[measure] =='largestContentfulPaint':
                page_median = client.query(f'SELECT median FROM "{measurement_list[measure]}" WHERE "summaryType"=\'pageSummary\' AND "statistics"=\'googleWebVitals\' {join_query}')
            else:
            # other metrics are gathered with page Summary and calculated median
                page_median = client.query(f'SELECT median FROM "{measurement_list[measure]}" WHERE "summaryType"=\'pageSummary\' AND {join_query}')
            page_median_list = list(page_median.get_points())
            
            for median_len in range(len(page_median_list)):
                dictionaery_page_median.update(dict(page_median_list[median_len]))
            # Median calculation
                for median in dictionaery_page_median.values():
                    if i%2 == 0:
                        pageSummary_median_list.append(median)
                        median_list_values.append(statistics.median(pageSummary_median_list))
                        median_page_summary = statistics.median(median_list_values)

            write_data_list.append("{measuremnt},{tag_set}release=GB median={summary_median} {time}".format(
            measuremnt = measurement_list[measure],
            summary_median = median_page_summary,
            query_tags = join_query_tags,
            tag_set = joined_tag_set_list,
            time = int(unix_time)
                )
            )
        else:
            for length in range(len(median_list)):
                dictionaery_median.update(dict(median_list[length]))

                for median in dictionaery_median.values():
                    i = i + 1
                    if i%2 == 0:
                        median_list_values.append(median)
                        median_summary = statistics.median(median_list_values)
            write_data_list.append("{measuremnt},{tag_set}release=GB median={summary_median} {time}".format(
            measuremnt = measurement_list[measure],
            summary_median = median_summary,
            query_tags = join_query_tags,
            tag_set = joined_tag_set_list,
            time = int(unix_time)
                )
            )
    
        # Command to add written list of queries:
        if args.test == True:    
            print("Shows where to and what will be added")
            print(write_data_list[0])       
            
        elif args.prod == True:
                print(write_data_list[0])   
                client.write_points(write_data_list, database=DB_name,protocol='line')
        else: print("Choose between --prod --test to execute or check ")    

def update_exec (write_data_list):
    client.write_points(write_data_list, database=DB_name,protocol='line')

# If --delete flag was added:
if args.delete == True:
    tags_delete(args.tag_key_list,args.tag_value_list)

# If --update flag was added:

if args.update == True:
    update_tags(args.tag_key_list,args.tag_value_list,
                args.insert_key, args.insert_value               
                )