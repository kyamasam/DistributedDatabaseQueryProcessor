# school-distributed-database

A simple implementation of distributed database using PostgreSQL and MySQL

Create a folder on your machine where you'd like to keep the project by running the command below

```
mkdir project
```

cd into the project directory

```
cd /<path_to_your_desired_location>/project/
```

make virtual environment `venv`

activate virtual environment
```
source venv/bin/activate
```

install requirements in the venv
```
pip3 install -r requirements.txt
```

To setup the servers, run the command below
```
fab deploy
```