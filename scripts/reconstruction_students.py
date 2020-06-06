# RECONSTRUCT STUDENTS FROM FRAGMENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def fetch_fragment_students_chiromo_records(site):
    students = []
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM students_chiromo
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print(f"Fragment students_chiromo successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2], row[3])
            students.append(row)
        return students

    except mysql.connector.Error as error:
        print("Failed to retrieve records from {}. ERROR: {}".format(site['application_wide_name'], error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED \n")


def fetch_fragment_students_kabete_records(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM students_kabete
        """

        cursor.execute(query)
        # result = cursor.fetchall()
        # print(f"{result}")
        students_records = cursor.fetchall()
        print("Fragment students_kabete successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2], row[3])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving fragment students_kabete", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site kabete closed \n")


print("JOINING FRAGMENTS")
time.sleep(2)
reconstructed_students_data = fetch_fragment_students_chiromo_records(
    site_chiromo
) + fetch_fragment_students_kabete_records(site_kabete)


def create_reconstructed_students_table(site):
    """Create a dhf table in MySQL db on site."""
    try:
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=site['host'],
            port="5432",
            database="school"
                                    )

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE reconstructed_students_table
            (
                ID   INT  UNIQUE NOT NULL,
                REGNO  VARCHAR  UNIQUE  NOT NULL ,
                CAMPUS VARCHAR NOT NULL,
                YEAROFSTUDY INT NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(f"Students table successfully reconstructed at {site['application_wide_name']}")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error reconstructing FEES table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection successfully closed \n")


create_reconstructed_students_table(master_students_db)


def insert_records_into_reconstructed_students_table(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgresql_insert_query = """
        INSERT INTO reconstructed_students_table (
        ID, REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s, %s, %s, %s)
        """

        cursor.executemany(postgresql_insert_query, records)
        connection.commit()
        print(
            cursor.rowcount,
            "Records inserted successfully into reconstructed_students_table"
        )

    except (Exception, psycopg2.Error) as error:
        print(
            "Failed to insert record into {} reconstructed table {}".format(
                site['application_wide_name'], error
            )
        )

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO {site} CLOSED \n")


print("RECONSTRUCTING STUDENTS TABLE")
time.sleep(2)
insert_records_into_reconstructed_students_table(
    reconstructed_students_data, master_students_db
)


def fetch_reconstructed_students_table(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM reconstructed_students_table ORDER BY ID
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("RECONSTRUCTED STUDENTS TABLE \n ID | regno | campus \t\t| year")
        for row in students_records:
            print(f"{row[0]} | {row[1]} | {row[2]}      | {row[3]}")
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving reconstructed_students_table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to master site closed \n")


print("FETCHING RECONSTRUCTED STUDENTS TABLE")
time.sleep(2)
fetch_reconstructed_students_table(master_students_db)
