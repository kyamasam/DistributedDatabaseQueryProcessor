import os
import sys

import mysql.connector
from mysql.connector import Error

sys.path.append(os.path.dirname(os.path.abspath('../data')))
from data.students_records import records

class DefaultMysqlParams:
    def __init__(self, user="admin",
                 password="admin",
                 host="52.58.90.32",
                 database="school"):
        self.user = "admin"
        self.password = "admin"
        self.host = "52.58.90.32"
        self.database = "school"


myDefaultMysqlParams = DefaultMysqlParams()


def connect_to_db(user=myDefaultMysqlParams.user, password=myDefaultMysqlParams.password,
                  host=myDefaultMysqlParams.host, database=myDefaultMysqlParams.database):
    try:
        connection = mysql.connector.connect(host=host,
                                             database=database,
                                             user=user,
                                             password=password)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

            return connection

    except Error as e:
        print("Error while connecting to MySQL", e)

        # connection should be closed by implementing function
connect_to_db()

def execute_query(query, user=myDefaultMysqlParams.user, password=myDefaultMysqlParams.password,
                  host=myDefaultMysqlParams.host, database=myDefaultMysqlParams.database):
    try:
        connection = connect_to_db(user=user, password=password, host=host, database=database)

        mySql_Create_Table_Query = query

        cursor = connection.cursor()
        result = cursor.execute(mySql_Create_Table_Query)
        print("Table created successfully ")

        return result

    except mysql.connector.Error as error:
        print("Failed to create table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def insert_query(query, records_to_insert, user=myDefaultMysqlParams.user, password=myDefaultMysqlParams.password,
                 host=myDefaultMysqlParams.host, database=myDefaultMysqlParams.database):
    try:
        connection = connect_to_db(user=user, password=password, host=host, database=database)

        mySql_insert_query = query

        records_to_insert = records_to_insert

        cursor = connection.cursor()
        cursor.executemany(mySql_insert_query, records_to_insert)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into table")

    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


# create table query
# execute_query(query='''CREATE TABLE students
#           (
#             ID SERIAL,
#             REGNO   VARCHAR(255) NOT NULL,
#             CAMPUS         TEXT      NOT NULL,
#             YEAROFSTUDY    INT       NOT NULL
#           ); ''')

# insert query
insert_query(query="""INSERT INTO students (REGNO, CAMPUS, YEAROFSTUDY)
                               VALUES (%s, %s, %s) """, records_to_insert= records)

# select query
# execute_query(query='''CREATE TABLE students
#           (
#             ID SERIAL,
#             REGNO   VARCHAR(255) NOT NULL,
#             CAMPUS         TEXT      NOT NULL,
#             YEAROFSTUDY    INT       NOT NULL
#           ); ''')
