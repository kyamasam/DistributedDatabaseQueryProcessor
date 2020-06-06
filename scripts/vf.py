# FRAGMENT STUDENTS

# import mysql.connector
import psycopg2
import time

from settings import DATABASES

site_departments = DATABASES['site_departments']
site_academics = DATABASES['site_academics']
master_students_db = DATABASES['master_students_db']


def vf_projects_supervision_table(site):
    """Perform a VF on fees table."""
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        query = "select id, regno, supervisor from projects"

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("students records successfully retrieved from projects")
        for row in students_records:
            print(row[0], row[1])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from projects table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to fees closed \n")


print("\n FETCHING STUDENT & ACADEMIC DETAILS")
project_supervision_records = vf_projects_supervision_table(master_students_db)


def vf_departments_projects(site):
    """Perform a VF on fees table."""
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        query = "select id, department, project_name from projects ORDER BY department"

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("students records successfully retrieved from projects")
        for row in students_records:
            print(row[0], row[1])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from projects table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to fees closed \n")


# print("\n FETCHING DEPARTMENT & PROJECT NAMES")
# project_supervision_records = vf_projects_supervision_table(master_students_db)


def create_vf_table_site_academics(site):
    """Create a vf table in site academics."""
    try:
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=site['host'],
            port="5432",
            database="school"
                                    )

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE project_supervision
            (
                ID  INT    UNIQUE    NOT NULL,
                REGNO  VARCHAR(20)  UNIQUE  NOT NULL ,
                SUPERVISOR VARCHAR NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(
            f"Fragment created successfully in {site['application_wide_name']}"
        )

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error creating table >> ", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to academics closed \n")


print("CREATING A VF TABLE IN ACADEMICS")
create_vf_table_site_academics(site_academics)


def create_vf_table_site_departments(site):
    """Create a vf table in site academics."""
    try:
        connection = psycopg2.connect(
            user="admin",
            password="admin",
            host=site['host'],
            port="5432",
            database="school"
                                    )

        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE departments_projects
            (
                ID  INT    UNIQUE    NOT NULL,
                PROJECT_NAME  VARCHAR(20)  UNIQUE  NOT NULL ,
                DEPARTMENTS VARCHAR NOT NULL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(
            f"Fragment created successfully in {site['application_wide_name']}"
        )

        return result

    except (Exception, psycopg2.Error) as error:
        print("Error creating table >> ", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Connection to departments closed \n")


print("CREATING A VH TABLE IN DEPARTMENTS")
create_vf_table_site_departments(site_departments)


# def create_dhf_fee_table_chiromo(site):
#     """Create a dhf table in MySQL db on site."""
#     try:
#         connection = mysql.connector.connect(
#             user="admin",
#             password="admin",
#             host=site['host'],
#             database="school"
#                                     )

#         if connection.is_connected():
#             db_Info = connection.get_server_info()
#             print(f"Connected to db {site}, MySQL Server version ", db_Info)
#         cursor = connection.cursor()

#         create_table_query = '''CREATE TABLE fragment_fee_chiromo
#             (
#                 ID    INT    NOT NULL    UNIQUE,
#                 REGNO  VARCHAR(20)    NOT NULL UNIQUE,
#                 FEE_BALANCE         REAL
#             ); '''

#         result = cursor.execute(create_table_query)
#         connection.commit()
#         print(f"Fragment created successfully in {site['application_wide_name']}")

#         return result

#     except mysql.connector.Error as error:
#         print("Failed to create fragment table in MySQL: {}".format(error))
#     finally:
#         if (connection.is_connected()):
#             cursor.close()
#             connection.close()
#             print(f"{site['application_wide_name']} MySQL connection is closed")


# create_dhf_fee_table_chiromo(site_chiromo)


# def insert_dhf_fee_kabete_records(records, site):
#     try:
#         connection = psycopg2.connect(user="admin",
#                                       password="admin",
#                                       host=site['host'],
#                                       port="5432",
#                                       database="school")
#         cursor = connection.cursor()

#         postgresql_insert_query = """
#         INSERT INTO fragment_fee_kabete (
#         ID, REGNO, FEE_BALANCE
#         ) VALUES (%s, %s, %s)
#         """

#         # for record in records:
#         #     cursor.execute(postgresql_insert_query, record)

#         cursor.executemany(postgresql_insert_query, records)
#         connection.commit()
#         print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

#     except (Exception, psycopg2.Error) as error:
#         print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

#     finally:
#         if (connection):
#             cursor.close()
#             connection.close()
#             print(f"CONNECTION TO FRAGMENT {site} CLOSED")


# insert_dhf_fee_kabete_records(fee_kabete, site_kabete)


# def insert_dhf_fee_chiromo_records(records, site):
#     try:
#         connection = mysql.connector.connect(user="admin",
#                                       password="admin",
#                                       host=site['host'],
#                                       database="school")
#         cursor = connection.cursor()

#         mysql_insert_query = """
#         INSERT INTO fragment_fee_chiromo (
#         ID, REGNO, FEE_BALANCE
#         ) VALUES (%s, %s, %s)
#         """

#         cursor.executemany(mysql_insert_query, records)
#         connection.commit()
#         print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

#     except mysql.connector.Error as error:
#         print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

#     finally:
#         if (connection.is_connected()):
#             cursor.close()
#             connection.close()
#             print(f"CONNECTION TO FRAGMENT {site} CLOSED")


# insert_dhf_fee_chiromo_records(fee_chiromo, site_chiromo)


# def fetch_fragment_fee_chiromo_records(site):
#     try:
#         connection = mysql.connector.connect(user="admin",
#                                       password="admin",
#                                       host=site['host'],
#                                       database="school")
#         cursor = connection.cursor()

#         query = """
#         SELECT * FROM fragment_fee_chiromo
#         """

#         cursor.execute(query)
#         result = cursor.fetchall()
#         print(result)

#     except mysql.connector.Error as error:
#         print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

#     finally:
#         if (connection.is_connected()):
#             cursor.close()
#             connection.close()
#             print(f"CONNECTION TO FRAGMENT {site} CLOSED")


# fetch_fragment_fee_chiromo_records(site_chiromo)


# def fetch_fragment_fee_kabete_records(site):
#     try:
#         connection = psycopg2.connect(user="admin",
#                                       password="admin",
#                                       host=site['host'],
#                                       database="school")
#         cursor = connection.cursor()

#         query = """
#         SELECT * FROM fragment_fee_kabete
#         """

#         cursor.execute(query)
#         result = cursor.fetchall()
#         print(f"{result}")

#     except (Exception, psycopg2.Error) as error:
#         print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

#     finally:
#         if (connection):
#             cursor.close()
#             connection.close()
#             print(f"CONNECTION TO FRAGMENT {site} CLOSED")


# fetch_fragment_fee_kabete_records(site_kabete)
