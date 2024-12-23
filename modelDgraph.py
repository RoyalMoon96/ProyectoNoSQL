#!/usr/bin/env python3
import datetime
from datetime import datetime
import json
import csv
import random
import pydgraph



def set_schema(client):
    schema = """
    type User {
        username
        real_name
        email
        age
        state
        friends
        tours
    }

    username: string @index(exact) .
    real_name: string @index(fulltext) .
    email: string @index(hash) .
    age: int .
    state: string @index(term) .
    friends: [uid] @reverse .
    tours: [uid] .

    type Tour {
        tour_name
        location
        duration_days
        price_per_person
        start_date
        end_date
        max_participants
        participants
        similar_tours
    }
    
    tour_name: string @index(exact) . 
    location: string .
    duration_days: int .
    price_per_person: float .
    start_date: datetime .
    end_date: datetime .
    max_participants: int .
    participants: [uid] .
    similar_tours: [uid] .

    """
    return client.alter(pydgraph.Operation(schema=schema))

def load_data(client):
    txn = client.txn()
    try:
        # Load users from CSV
        user_data = []
        with open("./users_data.csv", mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                user_data.append({
                    'uid': f'_:user_{row["username"]}',
                    'dgraph.type': 'User',
                    'username': row['username'],
                    'real_name': row['real_name'],
                    'email': row['email'],
                    'age': int(row['age']),
                    'state': row['state']
                })
        #print(user_data)

        # Load tours from CSV
        tour_data = []
        with open("./tours_data.csv", mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                tour_data.append({
                    'uid': f'_:tour_{row["tour_name"]}',
                    'dgraph.type': 'Tour',
                    'tour_name': row['tour_name'],
                    'location': row['location'],
                    'duration_days': int(row['duration_days']),
                    'price_per_person': float(row['price_per_person']),
                    'start_date': datetime.strptime(row['start_date'], "%Y-%m-%d %H:%M:%S.%f").isoformat(),
                    'end_date':datetime.strptime(row['end_date'], "%Y-%m-%d %H:%M:%S.%f").isoformat(),
                    'max_participants': int(row['max_participants']),
                })
        #print(tour_data)


        # Crear relaciones aleatorias entre usuarios y tours
        for user in user_data:
            # Seleccionar entre 1 y 3 tours al azar para cada usuario
            assigned_tours = random.sample(tour_data, k=random.randint(1, 3))
            user['tours'] = [{'uid': tour['uid']} for tour in assigned_tours]
            assigned_friends = random.sample(user_data, k=random.randint(1, 3))
            user['friends'] = [{'uid': friend['uid']} for friend in assigned_friends]


        for tour in tour_data:
            # Seleccionar entre 1 y 3 tours al azar para cada usuario
            assigned_tours = random.sample(tour_data, k=random.randint(1, 3))
            tour['similar_tours'] = [{'uid': tour_s['uid']} for tour_s in assigned_tours]
            assigned_users = random.sample(user_data, k=random.randint(1, 3))
            tour['participants'] = [{'uid': user['uid']} for user in assigned_users]

        # Mutate data into Dgraph
        data = user_data + tour_data
        #print("data:"+data)
        txn.mutate(set_obj=data)
        txn.commit()
        print("Data imported successfully.")
    finally:
        txn.discard()



def similar_tours(client, tour_name):
    query = """query similar_tours($tour_name: string) {
        all(func: eq(tour_name, $tour_name)) {
            uid
            tour_name
            similar_tours {
                uid
                tour_name
                location
                duration_days
                price_per_person
            }
        }
    }"""
    variables = {'$tour_name': tour_name}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Similar tours for {tour_name}: {json.dumps(data, indent=2)}")


def friend_tours(client, username):
    query = """query friend_tours($username: string) {
        all(func: eq(username, $username)) {
            uid
            username
            friends {
                username
                tours {
                    tour_name
                    location
                    start_date
                    end_date
                }
            }
        }
    }"""
    variables = {'$username': username}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Tours of friends for {username}: {json.dumps(data, indent=2)}")


def follows(client, username):
    query = """query user_relationships($username: string) {
        all(func: eq(username, $username)) {
            uid
            username
            friends {
                username
            }
            ~friends {
                username
            }
        }
    }"""
    variables = {'$username': username}
    res = client.txn(read_only=True).query(query, variables=variables)
    data = json.loads(res.json)
    print(f"Follows of {username}: {json.dumps(data, indent=2)}")



