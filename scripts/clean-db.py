# FRAGMENT STUDENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def delete_master_students_table(college):
    """Perform a DHF on fees table."""
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db['host'],
                                      port="5432",
                                      database="school")

        print("Deleting records from STUDENTS relation")
        time.sleep(3)
        cursor = connection.cursor()
        query = "DROP TABLE students CASCADE"

        result = cursor.execute(query)
        print("Master table students successfully deleted \n")
        return result

    except (Exception, psycopg2.Error) as error:
        print("Error deleting fragment_students_chiromo \n", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site chiromo closed \n \n")


delete_master_students_table(master_students_db)


def delete_fees_table(college):
    """Perform a DHF on fees table."""
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db['host'],
                                      port="5432",
                                      database="school")

        print("Deleting records from fees relation")
        time.sleep(3)
        cursor = connection.cursor()
        query = "DROP TABLE fees"

        result = cursor.execute(query)
        print("Table fees successfully deleted \n")
        return result

    except (Exception, psycopg2.Error) as error:
        print("Error deleting fees table \n", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to finance site closed \n \n")


delete_fees_table(master_students_db)


def delete_fragment_students_chiromo(site):
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

        query = '''DROP TABLE fragment_students_chiromo
            ; '''

        print("Deleting records from fragment students_chiromo")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment students_chiromo deleted successfully in {site['application_wide_name']}\n")

        return result

    except mysql.connector.Error as error:
        print("Failed to delete fragment table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed \n \n")


delete_fragment_students_chiromo(site_chiromo)


def delete_fragment_students_kabete(site):
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

        query = '''DROP TABLE fragment_students_kabete
            ; '''

        print("Deleting records from fragment students_kabete")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment students_kabete deleted successfully in {site['application_wide_name']} \n")

        return result

    except mysql.connector.Error as error:
        print("Failed to delete fragment table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed \n \n")


delete_fragment_students_kabete(site_kabete)


def delete_fragment_fee_kabete(site):
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

        query = '''DROP TABLE fragment_fee_kabete
            ; '''

        print("Deleting records from fragment fee_kabete")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment fee_kabete deleted successfully in {site['application_wide_name']} \n")

        return result

    except mysql.connector.Error as error:
        print("Failed to delete fragment table in MySQL: {} \n".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed \n \n")


def delete_fragment_fee_chiromo(site):
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

        query = '''DROP TABLE fragment_fee_chiromo
            ; '''

        print("Deleting records from fragment fee_chiromo")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment fee_chiromo deleted successfully in {site['application_wide_name']} \n")

        return result

    except mysql.connector.Error as error:
        print("Failed to delete fragment table in MySQL: {} \n".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed \n \n")


delete_fragment_fee_kabete(site_kabete)
delete_fragment_fee_chiromo(site_chiromo)
