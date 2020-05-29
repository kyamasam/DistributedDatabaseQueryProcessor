import os
import sys

from data.students_records import records, records_with_id
from settings import DATABASES
from utils.exceptions.exceptions import UnSupportedDialect

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
        return KeyError
    finally:
        pass
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
            # run the execute query found in the postgres functions file
            return postgres_execute_query(full_query, host=selected_db['host'])
        elif dialect == "MySQL":
            # run the execute query found in the mysql functions file
            return mysql_execute_query(full_query, host=selected_db['host'])
        else:
            # custom Exception
            raise UnSupportedDialect()
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
            raise UnSupportedDialect()


my_select_query = QueryObject("SELECT * ", "FROM students", " where campus = 'CHIROMO'")

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
my_insert_query = QueryObject("""INSERT INTO students """, """(ID, REGNO, CAMPUS, YEAROFSTUDY) """, """VALUES (%s, %s, %s)""" , records=records_with_id)



# run query in a site running PostgreSQL
# route_query(my_query, "site_kabete")

# run in a site running MySQL
# route_query(my_query_2, "site_kisumu")

# run in a mysql site
# route_query(my_insert_query, "site_kisumu")

# run in a PostgreSQL site
route_query(my_insert_query, "site_chiromo")
