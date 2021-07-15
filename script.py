import json  
import argparse
from influxdb import InfluxDBClient


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
update_group.add_argument('--insert_field_key','--ifk', help="insert key")
update_group.add_argument('--insert_field_value','--ifv', help="insert value")
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
client = InfluxDBClient(host=hostName, port=portNum, username=userName , password=userPassword, database=DB_name)

# With SSL conection: 
# client = InfluxDBClient(host=hostName, port=portNum, username=userName, password=userPassword, ssl=ssl_connection, verify_ssl=verify_ssl)


database_list = client.get_list_database()


# For user to switch beetween databases, taken from JSON config

client.switch_database(DB_name)

## Functions 

# Deletion function

def tags_delete(TARGkey,TARGval):
        key = list(TARGkey)
        value = list(TARGval)
        request_list = []
        for i in range(len(key)):
            join_request = f"\"{key[i]}\"" + "=" + '\'' + f"{value[i]}" + '\''

            if i >= 1:                   
                    and_request = " AND " + join_request
                    request_list.append(and_request)
            else:
                request_list.append(join_request)         
        join_query = ''.join(request_list)

        ## Separate join_query as a function (will be made in future versions)

        # Show before dropping:
        print(client.query(f"SHOW SERIES WHERE {join_query}"))
        # Drop data series:
        client.query(f"DROP SERIES WHERE {join_query}")
        print("Droping series executed")

# Update function

def update_tags(TARGkey,TARGval, update_tag_key, update_tag_value, update_field_key, update_field_value):
    key = list(TARGkey)
    value = list(TARGval)
    request_list = []
    for i in range(len(key)):
        join_request = f"\"{key[i]}\"" + "=" + '\'' + f"{value[i]}" + '\''

        if i >= 1:                   
                and_request = " AND " + join_request
                request_list.append(and_request)
        else:
            request_list.append(join_request)         
    join_query = ''.join(request_list)

    ## Seperate join_query as a function (will be made in future versions)

    # Gathering all measurements from given tag_items into a list:
    measurements_extract = list(client.query(f"SHOW MEASUREMENTS WHERE {join_query}"))
    measurement_list = measurements_extract[0]
    
    dictionery_measure = dict()
    write_data_list= []
    for l in range(len(measurement_list)):
        dictionery_measure.update(dict(measurements_extract[0][l]))

       # Running through all measurements and adding tags with fields:
        for key,value in dictionery_measure.items():
            write_data_list.append("{measurement},{tag_key}={tag_value} {field_key}={field_value}"
            .format(measurement=value,tag_key=update_tag_key[0],tag_value=update_tag_value[0],field_key=update_field_key,field_value=update_field_value))
    # Command to add written list of queries:
    client.write_points(write_data_list, database=DB_name,protocol='line')
    
# If --delete flag was added:

if args.delete == True:
    tags_delete(args.tag_key_list,args.tag_value_list)

# If --update flag was added:

if args.update == True:
    
    update_tags(args.tag_key_list,args.tag_value_list,
                args.insert_key, args.insert_value,
                args.insert_field_key, args.insert_field_value
                )