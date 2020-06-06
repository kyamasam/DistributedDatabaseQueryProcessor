# FRAGMENT STUDENTS

import mysql.connector
import psycopg2

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def phf_students_using_campus(college):
    """Perform a PHF using campus as predicate."""
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db['host'],
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from students where campus = %s"

        cursor.execute(postgreSQL_select_Query, (college,))
        students_records = cursor.fetchall()
        print(f"{college} students successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2], row[3])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from STUDENTS table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Records fetched successfully from MASTER STUDENTS relation \n")


kabete_students = phf_students_using_campus('KABETE')
chiromo_students = phf_students_using_campus('CHIROMO')


def create_phf_table_site_kabete(site):
    """Create fragment table in MySQL db on site."""
    try:
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=site['host'],
            port="5432",
            database="school"
                                    )

        # if connection.is_connected():
        #     db_Info = connection.get_server_info()
        #     print(f"Connected to db {site}, MySQL Server version ", db_Info)
        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE students_kabete
            (
                ID     INT  UNIQUE          NOT NULL ,
                REGNO  VARCHAR(20) UNIQUE   NOT NULL ,
                CAMPUS         TEXT      NOT NULL,
                YEAROFSTUDY    INT       NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(f"Fragment created successfully in {site['application_wide_name']}")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error creating table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site_kabete closed \n")


create_phf_table_site_kabete(site_kabete)


def create_phf_table_site_chiromo(site):
    """Create fragment table in MySQL db on site."""
    try:
        connection = mysql.connector.connect(
            user="admin",
            password="admin",
            host=site['host'],
            database="school"
                                    )

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print(f"Connected to db {site}, MySQL Server version ", db_Info)
        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE students_chiromo
            (
                ID     INT            NOT NULL UNIQUE,
                REGNO  VARCHAR(20)    NOT NULL UNIQUE,
                CAMPUS         TEXT      NOT NULL,
                YEAROFSTUDY    INT       NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(f"Fragment created successfully in {site['application_wide_name']}")

        return result

    except mysql.connector.Error as error:
        print("Failed to create fragment table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed")


create_phf_table_site_chiromo(site_chiromo)


def insert_phf_records_site_chiromo(records, site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO students_chiromo (
        ID, REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s, %s, %s, %s)
        """

        cursor.executemany(mysql_insert_query, records)
        connection.commit()
        print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

    except mysql.connector.Error as error:
        print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


insert_phf_records_site_chiromo(chiromo_students, site_chiromo)


def insert_phf_records_site_kabete(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO students_kabete (
        ID, REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s, %s, %s, %s)
        """

        cursor.executemany(mysql_insert_query, records)
        connection.commit()
        print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

    except (Exception, psycopg2.Error) as error:
        print("Error inserting data into fragment table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site kabete closed \n")


insert_phf_records_site_kabete(kabete_students, site_kabete)
