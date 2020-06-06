# FRAGMENT STUDENTS

import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_academics = DATABASES['site_academics']
site_departments = DATABASES['site_departments']
master_students_db = DATABASES['master_students_db']


def fetch_fragment_projects_academics_records(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM project_supervision
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("Academic records successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving fragment academics", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site academics closed \n")


def fetch_fragment_projects_departments_records(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM departments_projects
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("Department records successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error retrieving fragment academics", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to site academics closed \n")


print("JOINING FRAGMENTS")
time.sleep(2)
reconstructed_projects_data = fetch_fragment_projects_academics_records(
    site_academics
) + fetch_fragment_projects_departments_records(site_departments)

print(reconstructed_projects_data)


def create_reconstructed_projects_table(site):
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

        create_table_query = '''CREATE TABLE reconstructed_projects_table
            (
                ID INT UNIQUE NOT NULL,
                REGNO  VARCHAR NOT NULL ,
                PROJECT_NAME VARCHAR NOT NULL,
                DEPARTMENT VARCHAR NOT NULL,
                SUPERVISOR VARCHAR NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(
            "Projects table successfully reconstructed at master site \n"
        )

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error reconstructing projects table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection successfully closed \n")


create_reconstructed_projects_table(master_students_db)


def insert_records_into_reconstructed_fee_table(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgresql_insert_query = """
        INSERT INTO reconstructed_projects_table (
        ID, REGNO, PROJECT_NAME, DEPARTMENT, SUPERVISOR
        ) VALUES (%s, %s, %s, %s, %s)
        """

        cursor.executemany(postgresql_insert_query, records)
        connection.commit()
        print(
            cursor.rowcount,
            "Records inserted successfully into reconstructed projects table"
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


# print("RECONSTRUCTING FEES TABLE")
# time.sleep(2)
# insert_records_into_reconstructed_fee_table(
#     reconstructed_fees_data, master_students_db
# )


def fetch_reconstructed_projects_table(site):
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        query = """
        SELECT * FROM reconstructed_projects_table ORDER BY ID
        """

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("RECONSTRUCTED PROJECTS TABLE \n ID | regno | pname | dept | supervisor")
        for row in students_records:
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
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


print("FETCHING RECONSTRUCTED PROJECTS TABLE")
time.sleep(2)
fetch_reconstructed_projects_table(master_students_db)
