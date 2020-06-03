import os
import sys

import flask
from flask import request, jsonify

from data.students_records import records_with_id
from settings import DATABASES
from flask_cors import CORS, cross_origin

app = flask.Flask(__name__)
app.config["DEBUG"] = True
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

sys.path.append(os.path.dirname(os.path.abspath('utils/exceptions')))

from mysql_methods.mysql_functions import execute_query as mysql_execute_query, \
    insert_records_query as mysql_insert_records_query
from postgres_methods.postgres_functions import execute_query as postgres_execute_query, \
    insert_records_query as postgres_insert_records_query


# make query parameter an object
class QueryObject():

    def __init__(self, projection, cartesian_product, selection, records=[]):
        self.projection = projection
        self.selection = selection
        self.cartesian_product = cartesian_product
        self.records = records


# SELECT *
# FROM students
# WHERE campus = 'KABETE'
def route_query(query_object, target_database):
    # get the query string , main query function_type, target database,
    # get the target db
    selected_db = DATABASES
    try:
        selected_db = DATABASES[target_database]
        print("Using ", selected_db)
    except KeyError:
        print("Error Database could not be found or is not registered in settings.py")
        return "Error Database could not be found or is not registered in settings.py"
        # return KeyError
    finally:
        pass

    # get the query dialect
    dialect = selected_db['dialect']
    print(dialect)
    # concatenate query object
    full_query = ()
    full_query = query_object.projection + query_object.cartesian_product + query_object.selection
    query_method = str(query_object.projection).upper()
    # all these methods are supported within the exec query method
    if (query_method.find("SELECT")) != -1 or (query_method.find("DELETE")) != -1 or (
            query_method.find("CREATE")) != -1:
        print("use the exec query function method")
        if dialect == "PostgreSQL":
            print("Postegre dialect")
            # run the execute query found in the postgres functions file
            return postgres_execute_query(full_query, host=selected_db['host'], port=selected_db['port'],database=selected_db['database'], user=selected_db['user'], password=selected_db['password'])
        elif dialect == "MySQL":
            print("MySql dialect")
            # run the execute query found in the mysql functions file
            return mysql_execute_query(full_query, host=selected_db['host'],database=selected_db['database'], user=selected_db['user'], password=selected_db['password'])
        else:
            # custom Exception
            return "Unsupported Dialect"
            # raise UnSupportedDialect()
    elif (query_method.find("INSERT")) != -1:
        print("use insert method")
        if dialect == "PostgreSQL":

            # postgres syntax does not allow more than one %s
            full_query = query_object.projection + query_object.cartesian_product + "VALUES %s"
            # replace multiple %s with one %s
            # run the execute query found in the postgres functions file
            return postgres_insert_records_query(query_object.records, full_query, host=selected_db['host'])
        elif dialect == "MySQL":
            # run the execute query found in the mysql functions file
            return mysql_insert_records_query(full_query, query_object.records, host=selected_db['host'])
        else:
            # custom Exception
            return "Unsupported Dialect"
            # raise UnSupportedDialect()


my_select_query = QueryObject("SELECT * ", "FROM students_kabete", "")

# an example for create records
my_create_query = QueryObject(
    """CREATE TABLE students
             (
               ID SERIAL,
               REGNO   VARCHAR(255) NOT NULL,
               CAMPUS         TEXT      NOT NULL,
               YEAROFSTUDY    INT       NOT NULL
             );""", "", ""
)

# todo : Convert this query to allow user to concatenate records and
#  query to look more like an actual mysql query
# example of insert
my_insert_query = QueryObject("""INSERT INTO students """, """(ID, REGNO, CAMPUS, YEAROFSTUDY) """,
                              """VALUES (%s, %s, %s)""", records=records_with_id)


# run query in a site running PostgreSQL
# route_query(my_select_query, "site_kabete")

# run in a site running MySQL
# route_query(my_query_2, "site_kisumu")

# run in a mysql site
# route_query(my_insert_query, "site_kisumu")

# run in a PostgreSQL site
# route_query(my_insert_query, "site_chiromo")


@app.route('/', methods=['GET'])
@cross_origin()
def home():
    projection = request.args['projection']
    print(projection)
    cartesian_product = request.args['cartesian_product']
    print(cartesian_product)
    selection = request.args['selection']

    target_database = request.args['target_database']

    query = QueryObject(projection, cartesian_product, selection)

    result = route_query(query, target_database)

    return jsonify(result)


app.run()
