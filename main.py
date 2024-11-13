

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

    while True:
        pass



if __name__ == '__main__':
    main()
