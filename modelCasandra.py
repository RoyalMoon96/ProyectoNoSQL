#!/usr/bin/env python3
import logging
from datetime import datetime

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
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

################################################################
#   Q1
SELECT_USER_HISTORY = """
    SELECT tour_name, location, duration_days, price_per_person, start_date, end_date, max_participants
    FROM users_history
    WHERE username = ?
    ORDER BY start_date DESC;
"""

#################################################################
def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_TABLE_USERS_HISTORY)

#   Q1 
def get_user_history(session, username):
    log.info(f"Retrieving {username} previous tours history")
    try:
        # Use prepared statement for safer parameter binding
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
################################################################