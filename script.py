import json  
import influxdb
from influxdb import InfluxDBClient

# file_Path = input('Path to file:')
file_Path = '/home/perf/config.json'
file = open(file_Path,"r")
cred = json.loads(file.read())

pairs = cred["influxdb"].items()
l_json = list()
print('  Getting key, value:')
for key,value in pairs:
    l_json.append(value)

print(l_json)
userName = l_json[0]
userPassword = l_json[1]
hostName = l_json[2]
portNum = l_json[3]

file.close()
# Reading from JSON config END
 
# Entering Data base with values from JSON

# Localy checked connection to DB 
client = InfluxDBClient(host=hostName, port=portNum, username=userName , password=userPassword)

# More detailed connection configuration: 
# client = InfluxDBClient(host='hostName', port=portNum, username=userName, password=userPassword ssl=True, verify_ssl=True)


database_list = client.get_list_database()

print(database_list)

# Can be changed for user to switch beetween databases

client.switch_database('sitespeed')


tag_list = []

# Function forming a query request 

def tags(*TARGkey):
        key_value = list(TARGkey[0])
        request_list = []
        for i in range(len(key_value)):
            join_request = f"\"{key_value[i][0]}\"" + "=" + '\'' + f"{key_value[i][1]}" + '\''
            
            if i >= 1:                   
                    and_request = " AND " + join_request
                    print(and_request)
                    request_list.append(and_request)
            else:
                request_list.append(join_request)         
        join_query = ''.join(request_list)
        print(f"SHOW SERIES WHERE {join_query}")
        print(client.query(f"SHOW SERIES WHERE {join_query}"))


# Gathering user requests

rows = 0
cols = 1
tags_2D = []
while True:
    Tag_user_key = input("Enter desired Tag key: ")
    tag_list.append(Tag_user_key)
    if Tag_user_key == "":
        break
    else:
        Tag_user_value= input("Enter desired Tag value: ")
        rows =rows + 1
        for i in range (rows):
            next_tag = []
            for j in range(cols):
                next_tag.append(Tag_user_key)
                next_tag.append(Tag_user_value)                
        tags_2D.append(next_tag)
        
tags(tags_2D)