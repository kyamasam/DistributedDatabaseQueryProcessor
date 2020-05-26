# FRAGMENT STUDENTS

import psycopg2

from hosts import (
    fragment_kabete,
    master_students_db,
)

students = []


def fragment_students_by_campus(college):

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
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=fragment_kabete,
            port="5432",
            database="school"
                                    )

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE students_kabete
            (
                ID     INT       UNIQUE  NOT NULL,
                REGNO  VARCHAR UNIQUE    NOT NULL,
                CAMPUS         TEXT      NOT NULL,
                YEAROFSTUDY    INT       NOT NULL
            ); '''

        cursor.execute(create_table_query)
        connection.commit()
        print("Fragment students_kabete created successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating PostgreSQL table", error)
    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


create_fragment_table()


def insert_records(records):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=fragment_kabete,
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgres_insert_query = """
        INSERT INTO students_kabete (
        ID, REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s, %s, %s, %s)
        """
        for record in records:
            cursor.execute(postgres_insert_query, record)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into FRAGMENT KABETE")

    except (Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert records into FRAGMENT KABETE", error)

    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("FRAGMENT KABETE SUCCESSFULLY CREATED")


insert_records(students)
