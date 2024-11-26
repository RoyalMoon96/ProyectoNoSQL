#!/usr/bin/env python3
import json
import csv
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
    location: geo .  # Aseguramos compatibilidad con el tipo geo
    duration_days: int .
    price_per_person: float .
    start_date: datetime .
    end_date: datetime .
    max_participants: int .
    participants: [uid] .
    similar_tours: [uid] .
    """
    client.alter(pydgraph.Operation(schema=schema))

def insert_data_dgraph(client, user_csv, tours_csv):
    txn = client.txn()
    try:
        user_data = []
        tour_data = []

        # Cargar datos de usuarios
        with open(user_csv, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if not all(key in row for key in ["username", "real_name", "email", "age", "state"]):
                    print(f"Fila inválida en {user_csv}: {row}")
                    continue
                user_data.append({
                    'uid': f'_:user_{row["username"]}',
                    'dgraph.type': 'User',
                    'username': row['username'],
                    'real_name': row['real_name'],
                    'email': row['email'],
                    'age': int(row['age']),
                    'state': row['state']
                })

        # Cargar datos de tours
        with open(tours_csv, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if not all(key in row for key in ["tour_name", "location", "duration_days", "price_per_person", "start_date", "end_date", "max_participants"]):
                    print(f"Fila inválida en {tours_csv}: {row}")
                    continue
                try:
                    latitude, longitude = map(float, row['location'].split(','))
                except ValueError:
                    print(f"Coordenadas inválidas para {row['tour_name']}: {row['location']}")
                    continue
                tour_data.append({
                    'uid': f'_:tour_{row["tour_name"]}',
                    'dgraph.type': 'Tour',
                    'tour_name': row['tour_name'],
                    'location': {
                        'type': 'Point',
                        'coordinates': [longitude, latitude]
                    },
                    'duration_days': int(row['duration_days']),
                    'price_per_person': float(row['price_per_person']),
                    'start_date': row['start_date'],
                    'end_date': row['end_date'],
                    'max_participants': int(row['max_participants']),
                })

        # Insertar datos en Dgraph
        txn.mutate(set_obj=user_data + tour_data)
        txn.commit()
        print("Datos insertados correctamente en Dgraph.")
    except Exception as e:
        print(f"Error al insertar datos en Dgraph: {e}")
    finally:
        txn.discard()

def get_similar_tours(client, tour_name):
    try:
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
        if not data['all']:
            print(f"No se encontraron tours similares para {tour_name}.")
        else:
            print(f"Similar tours for {tour_name}: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"Error al obtener tours similares: {e}")

def get_friends_tours(client, username):
    try:
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
        if not data['all']:
            print(f"No se encontraron tours de amigos para {username}.")
        else:
            print(f"Tours of friends for {username}: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"Error al obtener tours de amigos: {e}")

def get_follows(client, username):
    try:
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
        if not data['all']:
            print(f"No se encontraron relaciones para {username}.")
        else:
            print(f"Followers and followings for {username}: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"Error al obtener relaciones de usuario: {e}")
#!/usr/bin/env python3
import datetime
import json
import csv
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
    location: geo .
    duration_days: int .
    price_per_person: float .
    start_date: datetime .
    end_date: datetime .
    max_participants: int .
    participants [uid] .
    similar_tours: [uid] .
}


    """
    return client.alter(pydgraph.Operation(schema=schema))

def load_data(client, user_csv, tours_data):
    txn = client.txn()
    try:
        # Load users from CSV
        with open(user_csv, mode='r') as file:
            reader = csv.DictReader(file)
            user_data = []
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

        # Load tours from CSV
        with open(tours_data, mode='r') as file:
            reader = csv.DictReader(file)
            tour_data = []
            for row in reader:
                tour_data.append({
                    'uid': f'_:tour_{row["tour_name"]}',
                    'dgraph.type': 'Tour',
                    'tour_name': row['tour_name'],
                    'location': {
                        'type': 'Point',
                        'coordinates': [float(row['longitude']), float(row['latitude'])]
                    },
                    'duration_days': int(row['duration_days']),
                    'price_per_person': float(row['price_per_person']),
                    'start_date': row['start_date'],
                    'end_date': row['end_date'],
                    'max_participants': int(row['max_participants']),
                })

        # Mutate data into Dgraph
        data = user_data + tour_data
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
    print(f"Followers and followings for {username}: {json.dumps(data, indent=2)}")



