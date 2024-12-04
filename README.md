# proyect NoSQL 

cassandra, mongodb and dgraph

### Setup a python virtual env with python mongodb installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 -m venv ./venv
source ./venv/bin/activate

# Install and activate virtual env (Windows)
python3 -m pip install virtualenv
python3 -m venv ./venv
.\venv\Scripts\Activate.ps1

# Install project python requirements
pip install -r requirements.txt
```

### To run the API service
```
python3 -m uvicorn mongoBack:app --reload
```

### To load data
Ensure you have the necessary running instances
i.e.:
```
docker run --name mongodb -d -p 27017:27017 mongo
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone
docker run --name node01 -p 9042:9042 -d cassandra
```
Once your API service is running (see step above)
```
python3 main.py
```
