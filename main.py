

#--------------------------------------------------------------------------------------
#------------------------------------ Cassandra ---------------------------------------
#--------------------------------------------------------------------------------------


import datetime
import pandas as pd
import logging
import os
from cassandra.cluster import Cluster

import modelCasandra

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('tours.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'tours')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def insert_data_cassandra(session):
    log.info("Loading data from CSV file")
    try:
        df = pd.read_csv('tours_users_df.csv')
        
        # Convert date columns to datetime objects
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        
        insert_users_history = session.prepare("""
            INSERT INTO users_history 
            (tour_name, location, duration_days, price_per_person, start_date, 
             max_participants, end_date, username, age, state, real_name, email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        for _, row in df.iterrows():
            # Insert into users_history
            session.execute(insert_users_history, (
                row['tour_name'],
                row['location'],
                int(row['duration_days']),
                float(row['price_per_person']),
                row['start_date'],  # Now a datetime object
                int(row['max_participants']),
                row['end_date'],    # Now a datetime object
                row['username'],
                int(row['age']),
                row['state'],
                row['real_name'],
                row['email']
            ))
            
        log.info("Data loaded successfully")
        print("Data loaded successfully!")
        
    except Exception as e:
        log.error(f"Error loading data: {str(e)}")
        print(f"Error loading data: {str(e)}")

#--------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------
#-------------------------------------- MONGO -----------------------------------------
#--------------------------------------------------------------------------------------
#!/usr/bin/env python3
import argparse
import requests
import csv
from datetime import datetime


# Read env vars related to API connection
MONGO_BASE_URL = "http://localhost:8000"
TOURS_API_URL = os.getenv("TOURS_API_URL", MONGO_BASE_URL)

def print_objects(o):
    for k in o.keys():
        print(f"{k}: {o[k]}")
    print("="*50)

def list_tours(start_date_From: str=None, start_date_To: str=None):
    suffix = "/tours/T"
    endpoint = TOURS_API_URL + suffix
    params = {
        "start_date_From": start_date_From,
        "start_date_To": start_date_To
    }
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for tour in json_resp:
            print_objects(tour)
    else:
        print(f"Error: {response}")
def insert_data_mongo():
    with open("./tours_data.csv") as fd:
        tours_csv = csv.DictReader(fd)
        for tour in tours_csv:
            tour["start_date"] = datetime.strptime(tour["start_date"], "%Y-%m-%d %H:%M:%S.%f").isoformat()
            tour["end_date"] = datetime.strptime(tour["end_date"], "%Y-%m-%d %H:%M:%S.%f").isoformat()
            x = requests.post(MONGO_BASE_URL+"/tours/T", json=tour)
            if not x.ok:
                print(f"Failed to post book {x} - {tour}")
    
    with open("./users_data.csv") as fd:
        users_csv = csv.DictReader(fd)
        for user in users_csv:
            x = requests.post(MONGO_BASE_URL+"/users/U", json=user)
            if not x.ok:
                print(f"Failed to post book {x} - {user}")

def user_info_mongo(limit: int=0, skip: int=0):
    suffix = "/users/U"
    endpoint = MONGO_BASE_URL + suffix
    params = {
        "limit": limit,
        "skip": skip,
    }
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for user in json_resp:
            print_objects(user)
    else:
        print(f"Error: {response}")

def get_tours_by_price_range(min_price: float=0, max_price: float=10000):
    suffix = "/tours/T"
    endpoint = TOURS_API_URL + suffix
    params = {
        "min_price": min_price,
        "max_price": max_price
    }
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for tour in json_resp:
            print_objects(tour)
    else:
        print(f"Error: {response}")

def get_tours_by_location(location: str=None):
    suffix = "/tours/T"
    endpoint = TOURS_API_URL + suffix
    params = {
        "location": location
    }
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for tour in json_resp:
            print_objects(tour)
    else:
        print(f"Error: {response}")

#--------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------
#-------------------------------------- Dgraph ----------------------------------------
#--------------------------------------------------------------------------------------
#!/usr/bin/env python3

import os
import pydgraph
import modelDgraph
#import set_schema, insert_data_dgraph, get_similar_tours, get_friends_tours, get_follows
#from modelDgraph import set_schema, insert_data_dgraph, get_similar_tours, get_friends_tours, get_follows

# Dgraph connection details
DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

#--------------------------------------------------------------------------------------
def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username

def print_menu():
    mm_options = {
        0: "Insert Data",
        1: "Show user info",        #Mongo
        2: "Show tours history",    #Cassandra
        3: "Show tours",            #Mongo, Dgraph
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_tours_menu():
    thm_options = {
        1: "All",                                       #Mongo
        2: "Date Range (Start and End date)",           #Mongo
        3: "Price per person",                          #Mongo
        4: "Locations",                                 #Mongo
        5: "Similar tours to (needs a name)",           #Dgraph
        6: "Contracted by friends",                     #Dgraph
        7: "Followers and Followings of friends",       #Dgraph
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def main():
    
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    modelCasandra.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    modelCasandra.create_schema(session)

    
    #Dgraph
        # Init Client Stub and Dgraph Client

    log.info("Connecting to Dgraph")
    client_stub = create_client_stub()
    client = create_client(client_stub)
    log.info("Setting schema in Dgraph")
    modelDgraph.set_schema(client)

    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    username = set_username()

    

    while True:
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            insert_data_mongo()
            try:
                modelDgraph.load_data(client)  # Dgraph
                print("Data inserted successfully!")
            except Exception as e:
                print(f"Error inserting data: {e}")  # Dgraph
        if option == 1:
            opt_limit="n"
            opt_limit = input("limit y/n: ").lower()
            limit=0
            if opt_limit == "y" or opt_limit =="yes":
                limit = int(input("limit value: "))

            opt_skip="n"
            opt_skip = input("skip y/n: ").lower()
            skip=0
            if opt_skip == "y" or opt_skip =="yes":
                skip = int(input("skip value: "))
            user_info_mongo(limit, skip)                                                 #Mongo
#        if option == 2:
#            get_user_history(session, username)                   #Cassandra
        if option == 3:
            print_tours_menu()
            tour_option = int(input('Enter your tours view choice: '))
            if tour_option == 1:
                list_tours()                             #Mongo
            #
            if tour_option == 2:
                print("Enter the start Date (exaple: '2025-09-04 09:55:17.905467')")
                S_date_from= input('From: ')
                S_date_to= input('To: ')
                list_tours(S_date_from, S_date_to)                             #Mongo
            #
            if tour_option == 3:
                min_price = float(input("Enter minimum price per person: "))
                max_price = float(input("Enter maximum price per person: "))
                get_tours_by_price_range(min_price, max_price)                  #Mongo
            #
            if tour_option == 4:
                print('Some available locations: "Paris", "New York", "Tokyo", "Sydney", "Rome", "London", "Barcelona"...')
                location = input("Enter location name: ").lower()
                get_tours_by_location(location)                                 #Mongo
            #
            if tour_option == 5:
                tour_name = input("Enter tour name to find similar tours: ")
                modelDgraph.similar_tours(client, tour_name)                            #Dgraph
            
            elif tour_option == 6:
                modelDgraph.friend_tours(client, username)       
                
            if tour_option == 7:
                modelDgraph.follows(client, username)                                   #Dgraph

        if option == 4:
            username = set_username()
        if option == 5:
            print("Thank you for using our tour application!")
            exit(0)
        else:
            print("Invalid option. Please try again.")
        


if __name__ == '__main__':
    main()



