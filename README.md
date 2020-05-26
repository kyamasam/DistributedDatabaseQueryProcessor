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

## Project Structure
![folder!](assets/icons/folder_sm.png) root_folder <br>
 >![folder!](assets/icons/folder_sm.png) scripts<br>
  >>![folder!](assets/icons/folder_sm.png) data<br>
  >>![folder!](assets/icons/folder_sm.png) mysql<br>
  >>![folder!](assets/icons/folder_sm.png) postgres<br>
  >>settings.py
  
  
 The `data` folder contains all static data required by the application<br>
 The `mysql` folder contains all the mysql related files
 The `postgres` folder contains all postgres related files
 The `settings` file contains all the databases that are used by the application
 
 #### Testing the files
 In order to test the database functions, uncomment the functions in the `example_transactions.py`