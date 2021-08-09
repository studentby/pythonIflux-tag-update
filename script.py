import json  
import argparse
from os import name, write
from time import time
from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet



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
        request_list.append(f"\"{key[i]}\"" + "=" + '\'' + f"{value[i]}" + '\'')        
    join_query = ' AND '.join(request_list)
    return join_query

# Deletion function

def tags_delete(TARGkey,TARGval):
    join_query = query(TARGkey,TARGval)
# Show Series to check
    if args.test == True:
        print(client.query(f"SHOW SERIES WHERE {join_query}"))
    elif args.prod ==True:
        (client.query(f"DROP SERIES WHERE {join_query}"))
        print("Droping series executed")
    # Drop data series:    
    else: print("Choose between --prod --test to execute or check ")

# Update function

def update_tags(TARGkey,TARGval, update_tag_key, update_tag_value):
    join_query = query(TARGkey,TARGval)

    # Gathering all measurements from given tag_items into a list:
    measurements_extract = list(client.query(f"SHOW MEASUREMENTS WHERE {join_query}"))
    raw_measurement = []

    dictionery_measure = dict()
    write_data_list= []
    field_data_list = []

    for l in range(len(measurements_extract[0])):
        dictionery_measure.update(dict(measurements_extract[0][l]))
        ## Field key extraction
        for field_key,field_val in dictionery_measure.items():
            rs=client.query(f'SHOW FIELD KEYS FROM "{field_val}"')

        ## Storing all measurements 
        raw_measurement.append(field_val)  
    field_data_list.append(list(rs.get_points()))
    
    ## Storing all field keys in a list
    store_field_key_list = []
    dictionaery_field_key = dict()
    for length in range(len(field_data_list[0])):
        dictionaery_field_key.update(field_data_list[0][length])
        for field_key_dict,field_val_dict in dictionaery_field_key.items():
            if field_val_dict !="float":
                store_field_key_list.append(field_val_dict)

    ## Get field values
    for l in range(len(measurements_extract[0])):
        dictionery_measure.update(dict(measurements_extract[0][l]))

       # Running through all measurements and adding tags with fields:
        for key,measurement_value in dictionery_measure.items():
            field_value_list = []
            for field_keys_in_list in range(len(store_field_key_list)):
                rs_field_values = client.query(f'SELECT {store_field_key_list[field_keys_in_list]} FROM "{measurement_value}"')
                rs_field_list = list(rs_field_values.get_points())
            # print(rs_field_list)
            
                for item in rs_field_list:
                    item[store_field_key_list[field_keys_in_list]]
                    
                    field_value_list.append(item[store_field_key_list[field_keys_in_list]])



                    write_data_list.append("{measurement},{tag_key}={tag_value} {field_key}={field_value}"
                    .format(measurement=measurement_value,
                    tag_key=update_tag_key[0],
                    tag_value=update_tag_value[0],
                    field_key=store_field_key_list[field_keys_in_list],
                    field_value=item[store_field_key_list[field_keys_in_list]]))

    






    # Command to add written list of queries:
    if args.test == True:    
        print("Shows where to and what will be added")
        print(write_data_list)

    elif args.prod == True:
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