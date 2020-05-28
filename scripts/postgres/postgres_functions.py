import os
import sys

import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.abspath('../data')))

from hosts import site_chiromo

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

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


connect_db(host=site_chiromo)


def exec_query(query, user="admin", password="admin", host="34.70.144.81", port="5432", database="school"):
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()
        postgreSQL_select_Query = query

        cursor.execute(postgreSQL_select_Query)
        connection.commit()
        try:
            print("Selecting rows from table ")
            query_results = cursor.fetchall()
            if not query_results:
                print("No records found")
            else:
                fields = ''
                for field in cursor.description:
                    fields += field[0] + ' | '
                print('\n', fields.upper(), '\n')
                for row in query_results:
                    values.append(row)
                    row_values = ''
                    for element in row:
                        row_values += str(element) + ' | '
                    print(row_values)
                    row_values = ''
                print('\n')
            return values
        except(Exception):
            # add a way of showing the result
            print("operation completed")


    except (Exception, psycopg2.Error) as error:
        print("Error", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def insert_records_query(records_to_insert, query, template=None, user="admin", password="admin", host="34.70.144.81",
                         port="5432",
                         database="school"):
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()
        # removed loop
        postgres_insert_query = query
        psycopg2.extras.execute_values(
            cursor, postgres_insert_query, records_to_insert, template=template, page_size=100
        )
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into students table")

    except (Exception, psycopg2.Error) as error:
        if (connection):
            print("Failed to insert records into students table", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
