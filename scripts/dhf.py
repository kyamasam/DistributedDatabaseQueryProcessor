# FRAGMENT STUDENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def dhf_fees_table(college):
    """Perform a DHF on fees table."""
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db['host'],
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        query = "select students.regno, fees.fee_balance from students inner join fees on students.regno = fees.regno where students.campus = %s"

        cursor.execute(query, (college,))
        students_records = cursor.fetchall()
        print(f"{college} students successfully retrieved")
        for row in students_records:
            print(row[0], row[1])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from STUDENTS table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Records fetched successfully relation \n")

print("\n FETCHING KABETE STUDENTS")
time.sleep(3)
fee_kabete = dhf_fees_table('KABETE')

print("\n FETCHING CHIROMO STUDENTS")
time.sleep(3)
fee_chiromo = dhf_fees_table('CHIROMO')


def create_dhf_fee_table_kabete(site):
    """Create a dhf table in MySQL db on site."""
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

        create_table_query = '''CREATE TABLE fragment_fee_kabete
            (
                REGNO  VARCHAR(20)    NOT NULL UNIQUE,
                FEE_BALANCE         REAL
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


create_dhf_fee_table_kabete(site_kabete)


def create_dhf_fee_table_chiromo(site):
    """Create a dhf table in MySQL db on site."""
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

        create_table_query = '''CREATE TABLE fragment_fee_chiromo
            (
                REGNO  VARCHAR(20)    NOT NULL UNIQUE,
                FEE_BALANCE         REAL
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


create_dhf_fee_table_chiromo(site_chiromo)


def insert_dhf_fee_kabete_records(records, site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO fragment_fee_kabete (
        REGNO,FEE_BALANCE
        ) VALUES (%s, %s)
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


insert_dhf_fee_kabete_records(fee_kabete, site_kabete)


def insert_dhf_fee_chiromo_records(records, site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO fragment_fee_chiromo (
        REGNO, FEE_BALANCE
        ) VALUES (%s, %s)
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


insert_dhf_fee_chiromo_records(fee_chiromo, site_chiromo)


def fetch_fragment_fee_chiromo_records(site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM fragment_fee_chiromo
        """

        cursor.execute(query)
        result = cursor.fetchall()
        print(result)

    except mysql.connector.Error as error:
        print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


fetch_fragment_fee_chiromo_records(site_chiromo)


def fetch_fragment_fee_kabete_records(site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM fragment_fee_kabete
        """

        cursor.execute(query)
        result = cursor.fetchall()
        print(f"{result}")

    except mysql.connector.Error as error:
        print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


fetch_fragment_fee_kabete_records(site_kabete)
