# FRAGMENT STUDENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def delete_master_students_table(site):
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

        query = '''DROP TABLE students
            ; '''

        print("Deleting records from master students table")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Students table deleted successfully in {site['application_wide_name']} \n")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error droping students table >>", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} connection is closed \n \n")


print("DROPING STUDENTS TABLE")
time.sleep(2)
delete_master_students_table(master_students_db)


def delete_fees_table(site):
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

        query = '''DROP TABLE fees
            ; '''

        print("Deleting records from fees table")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fees table deleted successfully in {site['application_wide_name']} \n")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error droping FEES table >>", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} connection is closed \n \n")


print("DROPING FEES TABLE")
time.sleep(2)
delete_fees_table(master_students_db)


def delete_projects_table(site):
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

        query = '''DROP TABLE projects
            ; '''

        print("Deleting records from projects table")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Projects table deleted successfully in {site['application_wide_name']} \n")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error droping projects table >>", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} connection is closed \n \n")


print("DROPING FEES TABLE")
time.sleep(2)
delete_projects_table(master_students_db)


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

        query = '''DROP TABLE students_chiromo
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


print("DROPING FRAGMENT STUDENTS_CHIROMO")
time.sleep(2)
delete_fragment_students_chiromo(site_chiromo)


def delete_fragment_students_kabete(site):
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

        query = '''DROP TABLE students_kabete
            ; '''

        print("Deleting records from fragment students_kabete")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment students_kabete deleted successfully in {site['application_wide_name']} \n")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error droping fragment table", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed \n \n")


print("DROPING FRAGMENT STUDENTS_KABETE")
time.sleep(2)
delete_fragment_students_kabete(site_kabete)


def delete_fragment_fee_kabete(site):
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

        query = '''DROP TABLE fragment_fee_kabete
            ; '''

        print("Deleting records from fragment fee_kabete")
        time.sleep(3)
        result = cursor.execute(query)
        connection.commit()
        print(f"Fragment fee_kabete deleted successfully in {site['application_wide_name']} \n")

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error deleting FEES table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection successfully closed \n")


print("DROPPING FRAGMENT FEE_KABETE")
delete_fragment_fee_kabete(site_kabete)


def delete_fragment_fee_chiromo(site):
    """DROP FEE FRAGMENT on site CHIROMO."""
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


print("DROPPING FRAGMENT FEE_CHIROMO")
delete_fragment_fee_chiromo(site_chiromo)


def delete_reconstructed_students_table(site):
    """Perform a DHF on fees table."""
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      #   host=master_students_db['host'],
                                      host=site['host'],
                                      port="5432",
                                      database="school")

        print("Deleting records from reconstructed STUDENTS relation")
        time.sleep(3)
        cursor = connection.cursor()
        query = "DROP TABLE reconstruted_students_table"

        result = cursor.execute(query)
        connection.commit()
        print("Reconstructed students table successfully deleted \n")
        return result

    except (Exception, psycopg2.Error) as error:
        print("Error deleting reconstructed_students_table >> \n", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to master site closed \n \n")


print("DROPPING RECONSTRUCTED STUDENTS TABLE")
delete_reconstructed_students_table(master_students_db)


def delete_reconstructed_fees_table(site):
    """Perform a DHF on fees table."""
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      #   host=master_students_db['host'],
                                      host=site['host'],
                                      port="5432",
                                      database="school")

        print("Deleting records from reconstructed fees relation")
        time.sleep(3)
        cursor = connection.cursor()
        query = "DROP TABLE reconstruted_fees_table"

        result = cursor.execute(query)
        connection.commit()
        print("reconstructed fees table successfully deleted \n")
        return result

    except (Exception, psycopg2.Error) as error:
        print("Error deleting reconstructed_fees_table \n", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to master site closed \n \n")


delete_reconstructed_fees_table(master_students_db)
