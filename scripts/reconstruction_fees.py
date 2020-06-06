# FRAGMENT STUDENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def fetch_fragment_fee_chiromo_records(site):
    students = []
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
        students_records = cursor.fetchall()
        print(f"Fragment fee_chiromo successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2])
            students.append(row)
        return students

    except mysql.connector.Error as error:
        print("Failed to insert record into {} MySQL table {}".format(
            site['application_wide_name'], error
        ))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED \n")


# fetch_fragment_fee_chiromo_records(site_chiromo)


def fetch_fragment_fee_kabete_records(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM fragment_fee_kabete
        """

        cursor.execute(query)
        # result = cursor.fetchall()
        # print(f"{result}")
        students_records = cursor.fetchall()
        print("FEE KABETE students successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving fragment fee_kabete", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site kabete closed \n")


# fetch_fragment_fee_kabete_records(site_kabete)

print("JOINING FRAGMENTS")
time.sleep(2)
reconstructed_fees_data = fetch_fragment_fee_chiromo_records(
    site_chiromo
) + fetch_fragment_fee_kabete_records(site_kabete)


def create_reconstructed_fee_table(site):
    """Reconstruct fee table in master db on site."""
    try:
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=site['host'],
            port="5432",
            database="school"
                                    )

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE reconstructed_fees_table
            (
                ID INT UNIQUE NOT NULL,
                REGNO  VARCHAR NOT NULL ,
                FEE_PAYMENT         REAL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(
            "Fee table successfully reconstructed at master site \n"
        )

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error reconstructing FEES table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection successfully closed \n")


create_reconstructed_fee_table(master_students_db)


def insert_records_into_reconstructed_fee_table(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgresql_insert_query = """
        INSERT INTO reconstructed_fees_table (
        ID, REGNO, FEE_PAYMENT
        ) VALUES (%s, %s, %s)
        """

        cursor.executemany(postgresql_insert_query, records)
        connection.commit()
        print(
            cursor.rowcount,
            "Records inserted successfully into reconstructed fees table"
        )

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into {} reconstructed table {}".format(
            site['application_wide_name'], error
        ))

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO {site} CLOSED")


print("RECONSTRUCTING FEES TABLE")
time.sleep(2)
insert_records_into_reconstructed_fee_table(
    reconstructed_fees_data, master_students_db
)


def fetch_reconstructed_fees_table(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM reconstructed_fees_table ORDER BY ID
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("RECONSTRUCTED STUDENTS TABLE \n ID | regno | fee_payment")
        for row in students_records:
            print(f"{row[0]} | {row[1]} | {row[2]}")
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving reconstructed_fees_table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to master site closed \n")


print("FETCHING RECONSTRUCTED FEES TABLE")
time.sleep(2)
fetch_reconstructed_fees_table(master_students_db)
