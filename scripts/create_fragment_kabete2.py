# FRAGMENT STUDENTS

import psycopg2
import mysql.connector
# from mysql.connector import Error

from hosts import (
    fragment_kabete,
    master_students_db,
)

students = []


def fragment_students_by_campus(college):
    """Fetch data from master postgres db where college = kabete."""
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db,
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from students where campus = %s"

        cursor.execute(postgreSQL_select_Query, (college,))
        students_records = cursor.fetchall()
        for row in students_records:
            print(row[0], row[1], row[2], row[3])
            students.append(row)
        return 'students_records'

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from STUDENTS table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Records fetched successfully from STUDENTS relation \n")
        return 'students_records'


fragment_students_by_campus('KABETE')


def create_fragment_table():
    try:
        connection = mysql.connector.connect(
            user="admin",
            password="admin",
            host=fragment_kabete,
            # port="5432",
            database="school"
                                    )

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to db KABETE MySQL Server version ", db_Info)
        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE student_kabetes
            (
                ID     INT            NOT NULL UNIQUE,
                REGNO  VARCHAR(20)    NOT NULL UNIQUE,
                CAMPUS         TEXT      NOT NULL,
                YEAROFSTUDY    INT       NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print("Fragment students_kabete created successfully")

        return result

    except mysql.connector.Error as error:
        print("Failed to create fragment table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


create_fragment_table()


def insert_records(records):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=fragment_kabete,
                                      #   port="5432",
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO student_kabetes (
        ID, REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s, %s, %s, %s)
        """

        cursor.executemany(mysql_insert_query, students)
        connection.commit()
        print(cursor.rowcount, "Records inserted successfully into fragment_kabete")

    except mysql.connector.Error as error:
        print("Failed to insert record into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("CONNECTION TO FRAGMENT KABETE CLOSED")


insert_records(students)
