#!/usr/bin/env python3
import logging
from datetime import datetime

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_TABLE_USERS = """
    CREATE TABLE IF NOT EXISTS users (
        username TEXT,
        age INT,
        state TEXT,
        real_name TEXT,
        email TEXT,
        PRIMARY KEY ((username))
    )
"""


CREATE_TABLE_USERS_HISTORY = """
    CREATE TABLE IF NOT EXISTS users_history (
        tour_name TEXT,
        location TEXT,
        duration_days INT,
        price_per_person FLOAT,
        start_date TIMESTAMP,
        max_participants INT,
        end_date TIMESTAMP,
        username TEXT,
        age INT,
        state TEXT,
        real_name TEXT,
        email TEXT,
        PRIMARY KEY ((username), start_date)
    )
"""

CREATE_TABLE_TOURS_DAYS = """
    CREATE TABLE IF NOT EXISTS tours_duration (
        tour_name TEXT,
        location TEXT,
        duration_days INT,
        price_per_person FLOAT,
        start_date TIMESTAMP,
        max_participants INT,
        end_date TIMESTAMP,
        username TEXT,
        age INT,
        state TEXT,
        real_name TEXT,
        email TEXT,
        PRIMARY KEY ((duration_days), tour_name)
    )
"""

################################################################
#   Q1
SELECT_USER_INFO = """
    SELECT username, age, state, real_name, email
    FROM users
    WHERE username = ?
"""

#   Q2
SELECT_USER_HISTORY = """
    SELECT tour_name, location, duration_days, price_per_person, start_date, end_date, max_participants
    FROM users_history
    WHERE username = ?
    ORDER BY start_date DESC;
"""

#   Q3
SELECT_TOURS_DURATION = """
    SELECT tour_name, location, duration_days, price_per_person, start_date, end_date, max_participants
    FROM tours_duration
    WHERE duration_days = ?
"""

#################################################################
def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_TABLE_USERS)
    session.execute(CREATE_TABLE_USERS_HISTORY)
    session.execute(CREATE_TABLE_TOURS_DAYS)

#   Q1 
def get_user_info(session, username):
    log.info(f"Retrieving {username} info")
    try:
        prepared_stmt = session.prepare(SELECT_USER_INFO)
        rows = session.execute(prepared_stmt, [username])
        
        if not rows:
            print(f"No info found for user {username}")
            return
        
        print(f"\nUser {username} info:")
        print("=" * 80)
        for row in rows:
            print(f"\nUsername: {row.username}")
            print(f"Age: {row.age}")
            print(f"State: {row.state}")
            print(f"Name: {row.real_name}")
            print(f"Email: {row.email}")
            print("-" * 40)
            
    except Exception as e:
        log.error(f"Error retrieving user info: {str(e)}")
        print(f"Error retrieving user info: {str(e)}")

#   Q2
def get_user_history(session, username):
    log.info(f"Retrieving {username} previous tours history")
    try:

        prepared_stmt = session.prepare(SELECT_USER_HISTORY)
        rows = session.execute(prepared_stmt, [username])
        
        if not rows:
            print(f"No tours found for user {username}")
            return
        
        print(f"\nTours history for user {username}:")
        print("=" * 80)
        for row in rows:
            print(f"\nTour: {row.tour_name}")
            print(f"Location: {row.location}")
            print(f"Duration: {row.duration_days} days")
            print(f"Price: ${row.price_per_person:.2f}")
            print(f"Start Date: {row.start_date}")
            print(f"End Date: {row.end_date}")
            print(f"Max Participants: {row.max_participants}")
            print("-" * 40)
            
    except Exception as e:
        log.error(f"Error retrieving user history: {str(e)}")
        print(f"Error retrieving user history: {str(e)}")


#   Q3
def list_tours_duration(session, duration):
    log.info(f"Retrieving tours with {duration} days duration")
    try:
        prepared_stmt = session.prepare(SELECT_TOURS_DURATION)
        rows = session.execute(prepared_stmt, [int(duration)])
        
        if not rows:
            print(f"No tours found for {duration} days duration")
            return
        
        print(f"\nTours available:")
        print("=" * 80)
        for row in rows:
            print(f"\nTour: {row.tour_name}")
            print(f"Location: {row.location}")
            print(f"Duration: {row.duration_days} days")
            print(f"Price: ${row.price_per_person:.2f}")
            print(f"Start Date: {row.start_date}")
            print(f"End Date: {row.end_date}")
            print(f"Max Participants: {row.max_participants}")
            print("-" * 40)
            
    except Exception as e:
        log.error(f"Error retrieving tours: {str(e)}")
        print(f"Error retrieving tours: {str(e)}")
################################################################