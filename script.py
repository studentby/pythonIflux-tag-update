import json  
import argparse
from typing import Dict
from influxdb import InfluxDBClient


## Gathering info from JSON config file:

# Take arguments from terminal

parser = argparse.ArgumentParser()

parser.add_argument("--config", help="config file absolute path",default="./config.json")
parser.add_argument("--tag_key","--k", action='append',dest='tag_key_list', help="tag key")
parser.add_argument("--tag_value","--v" ,action='append',dest='tag_value_list' ,help="tag value")

delete_group = parser.add_argument_group('delete_group')
delete_group.add_argument('--delete', action='store_true',default=False)

update_group = parser.add_argument_group('update_group')
update_group.add_argument('--update', action='store_true',default=False)

args = parser.parse_args()


# file_Path taken from terminal 
file_Path = args.config
file = open(file_Path,"r")
cred = json.loads(file.read())

# Extracting only from InfluxDB tree

pairs = cred["influxdb"].items()
l_json = []
# print('  Getting key, value:')

for key,value in pairs:
    l_json.append(value)

# Taking values from exact position

print(l_json)
userName = l_json[0]
userPassword = l_json[1]
hostName = l_json[2]
portNum = l_json[3]
DB_name = l_json[4]
ssl_connection = l_json[5]
verify_ssl = l_json[6]

userName=cred["influxdb"]["username"]
userPassword=cred["influxdb"]["password"]
hostName=cred["influxdb"]["host"]
portNum=cred["influxdb"]["port"]
DB_name=cred["influxdb"]["database"]
ssl_connection=cred["influxdb"]["SSL"]
verify_ssl=cred["influxdb"]["verify_ssl"]


file.close()
# Reading from JSON config END

## Entering Data base with values from JSON

# Localy checked connection to DB 
client = InfluxDBClient(host=hostName, port=portNum, username=userName , password=userPassword)

# SSL conection: 
# client = InfluxDBClient(host=hostName, port=portNum, username=userName, password=userPassword ssl=ssl_connection, verify_ssl=verify_ssl)


database_list = client.get_list_database()


# For user to switch beetween databases, taken from JSON config

client.switch_database(DB_name)


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
        # Show before dropping
        # print(client.query(f"SHOW SERIES WHERE {join_query}"))

        ## need to create separeted function for update operation
        measurements_extract = list(client.query(f"SHOW MEASUREMENTS WHERE {join_query}"))
        measurement_list = measurements_extract[0]
        print(len(measurement_list))
        dictionery_measure = dict()
        print(measurement_list)
        l_m = []
        for l in range(len(measurement_list)):
            dictionery_measure.update(dict(measurements_extract[0][l]))
            for key,value in dictionery_measure.items():
                l_m.append(value)
            print(l_m)

                # print(dictionery_measure)

        # Drop data series
        # client.query(f"DROP SERIES WHERE {join_query}")
        # print("Droping series executed")



# If --delete flag was added:

if args.delete == True:
    tags_delete(args.tag_key_list,args.tag_value_list)

# If --update flag was added

# if args.update == True:






# Gathering user requests

# rows = 0
# cols = 1
# tags_2D = []
# while True:
#     Tag_user_key = input("Enter desired Tag key: ")
#     tag_list.append(Tag_user_key)
#     if Tag_user_key == "":
#         break
#     else:
#         Tag_user_value= input("Enter desired Tag value: ")
#         rows =rows + 1
#         for i in range (rows):
#             next_tag = []
#             for j in range(cols):
#                 next_tag.append(Tag_user_key)
#                 next_tag.append(Tag_user_value)                
#         tags_2D.append(next_tag)

# Calling a function to make query request 
# tags(tags_2D)
