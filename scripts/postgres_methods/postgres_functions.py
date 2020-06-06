import os
import sys
from pprint import pprint

import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.abspath('../data')))

values = []


def connect_db(user="admin", password="admin", host="34.70.144.81", port="5432", database="school"):
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return connection

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return error
    finally:
        # connection is closed by caller
        pass


# connect_db(host=site_chiromo)

def execute_query(query,host="34.70.144.81", user="admin", password="admin", port="5432", database="school"):
    try:
        connection = connect_db(user=user, password=password, host=host, port=port, database=database)
        cursor = connection.cursor()
        postgreSQL_select_Query = query
        print("qur", postgreSQL_select_Query)

        cursor.execute(postgreSQL_select_Query)
        connection.commit()
        try:
            print("Selecting rows from table ")
            query_results = cursor.fetchall()
            if not query_results:
                print("No records found")
                return "No records found"
            else:
                pprint(query_results)

            col_names = [field[0] for field in cursor.description]
            # add the query to the last row
            query_results_array = []
            query_results_array=(query_results)
            query_results_array.append(postgreSQL_select_Query)
            query_results.insert(0, col_names)

            return query_results_array
        except Exception as e:
            # add a way of showing the result
            print("operation completed")
            print(e)
            return "An Error Occurred, Check Application Console for trace"

    except (Exception, psycopg2.Error) as error:
        print("Error", error)
        return "An Error Occurred, Check Application Console for trace"

    finally:
        # closing database connection.
        try:
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
        except Exception as e:
            return "An Error Occurred, Check Application Console for trace"


def insert_records_query(records_to_insert, query, template=None, user="admin", password="admin", host="34.70.144.81",
                         port="5432",
                         database="school"):
    print(query)
    try:
        connection = connect_db()
        cursor = connection.cursor()
        # removed loop
        postgres_insert_query = query
        psycopg2.extras.execute_values(
            cursor, postgres_insert_query, records_to_insert, template=template, page_size=100
        )
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into table")

    except (Exception, psycopg2.Error) as error:
        if (connection):
            print("Failed to insert records into students table", error)
            return "Failed to insert records into students table"

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
