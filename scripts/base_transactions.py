from settings import DATABASES
import psycopg2
# import mysql.connector

from data.students_records import (
    records, fee_records, project_records
)

from postgres_methods.postgres_functions import (
    execute_query, insert_records_query, connect_db
)

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def create_master_students_table():
    execute_query(query='''CREATE TABLE students
                         (
                           ID SERIAL,
                           REGNO   VARCHAR(255) PRIMARY KEY,
                           CAMPUS         TEXT      NOT NULL,
                           YEAROFSTUDY    INT       NOT NULL
                         ); '''
                  )


create_master_students_table()


def insert_to_master():
    insert_records_query(records_to_insert=records, query="""
            INSERT INTO students (
            REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """)


insert_to_master()


def fetch_master_data():
    execute_query(query="select * from students")


fetch_master_data()


def create_fees_table():
    execute_query(query='''
               CREATE TABLE fees
                         (
                           ID SERIAL PRIMARY KEY,
                           REGNO  VARCHAR(255) REFERENCES students(REGNO),
                           FEE_PAYMENT         REAL
                         ); '''
                  )


create_fees_table()


def insert_to_fees():
    insert_records_query(records_to_insert=fee_records, query="""
            INSERT INTO fees (
            REGNO, FEE_PAYMENT
            ) VALUES %s
            """)


insert_to_fees()


def fetch_fee_data():
    execute_query(query="select * from fees")


fetch_fee_data()


def create_projects_table():
    execute_query(query='''
               CREATE TABLE projects
                         (
                           ID SERIAL PRIMARY KEY,
                           REGNO  VARCHAR(255) REFERENCES students(REGNO),
                           PROJECT_NAME  VARCHAR  NOT NULL,
                           PROJECT_TYPE VARCHAR   NOT NULL,
                           SUPERVISOR VARCHAR     NOT NULL
                         ); '''
                  )


create_projects_table()


def insert_to_projects():
    insert_records_query(records_to_insert=project_records, query="""
            INSERT INTO projects (
            REGNO, PROJECT_NAME, PROJECT_TYPE, SUPERVISOR
            ) VALUES %s
            """)


insert_to_projects()


def fetch_projects_data():
    execute_query(query="select * from projects")


fetch_projects_data()


def update_student_record(student_id, campus):
    try:
        connection = connect_db()

        cursor = connection.cursor()

        print("Table Before updating record ")
        sql_select_query = """select * from students where id = %s"""
        cursor.execute(sql_select_query, (student_id,))
        record = cursor.fetchone()
        print(record)

        # Update single record now
        sql_update_query = """
        Update students set CAMPUS = %s where id = %s"""
        cursor.execute(sql_update_query, (campus, student_id))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record Updated successfully ")

        print("Table After updating record ")
        sql_select_query = """select * from students where id = %s"""
        cursor.execute(sql_select_query, (student_id,))
        record = cursor.fetchone()
        print(record)

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# update_student_record(2, 'Chiromo')
