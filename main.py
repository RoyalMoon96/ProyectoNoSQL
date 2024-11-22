

#--------------------------------------------------------------------------------------
#------------------------------------ Cassandra ---------------------------------------
#--------------------------------------------------------------------------------------
import random
import uuid
import datetime
import time_uuid

from cassandra.cluster import Cluster

import modelCasandra

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')
#--------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------
#-------------------------------------- MONGO -----------------------------------------
#--------------------------------------------------------------------------------------
#!/usr/bin/env python3
import argparse
import logging
import os
import requests


# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('books.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to API connection
BOOKS_API_URL = os.getenv("BOOKS_API_URL", "http://localhost:8000")

def print_tours(tours):
    for k in tours.keys():
        print(f"{k}: {tours[k]}")
    print("="*50)

def list_tours(start_date):
    suffix = "/book"
    endpoint = BOOKS_API_URL + suffix
    params = {
        "start_date": start_date
    }
    response = requests.get(endpoint, params=params)
    if response.ok:
        json_resp = response.json()
        for book in json_resp:
            print_tours(book)
    else:
        print(f"Error: {response}")
#--------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------
#-------------------------------------- Dgraph ----------------------------------------
#--------------------------------------------------------------------------------------
#!/usr/bin/env python3
import os

import pydgraph

import model

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
        4: "Activities",                                #Mongo
        5: "Similar tours to (needs a name)",           #Dgraph
        6: "Contracted by friends",                     #Dgraph
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

    username = set_username()
    
    #Dgraph
        # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.set_schema(client)

    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    while True:
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            user_info()                                                 #Mongo
        if option == 2:
            model.get_user_history(session, username)                   #Cassandra
        if option == 3:
            print_tours_menu()
            tour_option = int(input('Enter your tours view choice: '))
            if tour_option == 1:
                list_tours(args.start_date)                             #Mongo
            #
            if tour_option == 2:
                print("Please enter dates in the format YYYY-MM-DD")
                start_date = input("Enter start date: ")
                end_date = input("Enter end date: ")    
                get_tours_by_date_range(start_date, end_date)                   #Mongo
            #
            if tour_option == 3:
                min_price = float(input("Enter minimum price per person: "))
                max_price = float(input("Enter maximum price per person: "))
                get_tours_by_price_range(min_price, max_price)                  #Mongo
            #
            if tour_option == 4:
                print("Available activities: hiking, swimming, sightseeing, cultural, adventure")
                activity = input("Enter activity type: ").lower()
                get_tours_by_activity(activity)                                 #Mongo
            #
            if tour_option == 5:
                tour_name = input("Enter tour name to find similar tours: ")
                get_similar_tours(client, tour_name)                            #Dgraph
            #
            elif tour_option == 6:
                get_tours_hired_by_friends(client, username)               #Dgraph

        if option == 4:
            username = set_username()
        if option == 5:
            print("Thank you for using our tour application!")
            exit(0)
        else:
            print("Invalid option. Please try again.")



if __name__ == '__main__':
    main()
