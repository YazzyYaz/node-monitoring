import requests
import json
import pandas as pd
import numpy as np
import pprint
from ip2geotools.databases.noncommercial import DbIpCity
import re
from collections import Counter
import time
import os.path
from os import path

re2='.*?'	# Non-greedy match on filler
re3='(\\d+)'	# Integer Number 1
re4='.*?'	# Non-greedy match on filler
re5='(\\d+)'	# Integer Number 2
re6='.*?'	# Non-greedy match on filler
re7='(\\d+)'	# Integer Number 3
re8='.*?'	# Non-greedy match on filler
re9='(\\d+)'	# Integer Number 4

rg = re.compile(re2+re3+re4+re5+re6+re7+re8+re9,re.IGNORECASE|re.DOTALL)

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

url = """
https://www.ethernodes.org/network/1/data?draw=1&columns[0][data]=id&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=host&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=port&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=country&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=clientId&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=client&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=true&columns[5][search][value]=&columns[5][search][regex]=false&columns[6][data]=clientVersion&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=true&columns[6][search][value]=&columns[6][search][regex]=false&columns[7][data]=os&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=true&columns[7][search][value]=&columns[7][search][regex]=false&columns[8][data]=lastUpdate&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=true&columns[8][search][value]=&columns[8][search][regex]=false&order[0][column]=0&order[0][dir]=asc&start=0&length=1000000&search[value]=&search[regex]=false&_=1545071939943 
    """
node_count = Counter()
node_chart = {
    'Oregon': [],
    'Washington': []
} 

if path.exists("nodes.json"):
    df = pd.read_json("nodes.json")
else:
    response = requests.get(url)
    response_hash = json.loads(response.text).get('data')
    df = pd.DataFrame.from_dict(response_hash)
    df.to_json("nodes.json")
print(len(df.index))
us_df = df.loc[df['country'] == 'United States']
len_us = int(len(us_df.index))
print(len_us)
print(us_df.head)

def multiples(m, count):
    row_list = []
    for i in range(count):
        row_list.append(m*i)
    return row_list

row_list = multiples(100, 428)
pprint.pprint(row_list)

for index, row in us_df.iterrows():
    ip_address = row['host']
    check_aws_regex = rg.search(ip_address)
    if ('linode' in ip_address) or ('spectrum' in ip_address):
        print('Linode/Spectrum node found')
    elif check_aws_regex:
        signed_int1=check_aws_regex.group(1)
        signed_int2=check_aws_regex.group(2)
        signed_int3=check_aws_regex.group(3)
        signed_int4=check_aws_regex.group(4)
        ip_address = f'{signed_int1}.{signed_int2}.{signed_int3}.{signed_int4}'
    try:
        if index in row_list:
            pprint.pprint(node_count)
            with open(f"row_{index}.json", "w") as outfile:
                json.dump(node_chart, outfile)
        response = DbIpCity.get(ip_address, api_key='free')
        city = response.region
        node_count[city] += 1
        if city == 'Washington' or city == 'Oregon':
            node_map = {}
            node_map['Node IP Address'] = ip_address
            node_map['Node ID'] = row['id']
            node_chart[city].append(node_map)
    except Exception as e:
        print(f"Error at {e}")
        print(ip_address)
        continue
pprint.pprint(node_count)
pprint.pprint(node_chart)
