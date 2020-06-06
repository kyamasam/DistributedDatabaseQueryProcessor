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
            print(row[0], row[1], row[2])
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
        query = "select id, project_name, department from projects ORDER BY department"

        cursor.execute(query)
        students_records = cursor.fetchall()
        print("students records successfully retrieved from projects")
        for row in students_records:
            print(row[0], row[1], row[2])
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


print("\n FETCHING DEPARTMENT & PROJECT NAMES")
departments_projects = vf_departments_projects(master_students_db)


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
                DEPARTMENT VARCHAR NOT NULL
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


def insert_vf_records_academics(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgresql_insert_query = """
        INSERT INTO project_supervision (
        ID, REGNO, SUPERVISOR
        ) VALUES (%s, %s, %s)
        """

        # for record in records:
        #     cursor.execute(postgresql_insert_query, record)

        cursor.executemany(postgresql_insert_query, records)
        connection.commit()
        print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into {} table {}".format(site['application_wide_name'], error))

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


insert_vf_records_academics(project_supervision_records, site_academics)


def insert_vf_records_departments(records, site):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgresql_insert_query = """
        INSERT INTO departments_projects (
        ID, PROJECT_NAME, DEPARTMENT
        ) VALUES (%s, %s, %s)
        """

        # for record in records:
        #     cursor.execute(postgresql_insert_query, record)

        cursor.executemany(postgresql_insert_query, records)
        connection.commit()
        print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into {} departments table {}".format(site['application_wide_name'], error))

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


insert_vf_records_departments(departments_projects, site_departments)


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
