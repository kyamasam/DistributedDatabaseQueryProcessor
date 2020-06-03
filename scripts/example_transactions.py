from query_router import route_query
from settings import DATABASES
import psycopg2

from data.students_records import records

from postgres_methods.postgres_functions import execute_query, insert_records_query, connect_db

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def create_fragment_chiromo():
    students_records = execute_query(query="select * from students where campus = 'CHIROMO'", host=master_students_db['host'])
    execute_query(query=
               '''CREATE TABLE students_chiromo
                  (
                      ID     INT     UNIQUE    NOT NULL,
                      REGNO  VARCHAR UNIQUE    NOT NULL,
                      CAMPUS         TEXT      NOT NULL,
                      YEAROFSTUDY    INT       NOT NULL
                  ); '''
                  , host=site_chiromo['host'])

    insert_records_query(records_to_insert=students_records, query="""
            INSERT INTO students_chiromo (
            ID, REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """, host=site_chiromo['host'])


# create_fragment_chiromo()


def create_fragment_kabete():
    students = []
    students_records = execute_query(query="select * from students where campus = 'KABETE'", host=master_students_db['host'])
    execute_query(query=
               '''CREATE TABLE students_kabete
                  (
                      ID     INT     UNIQUE    NOT NULL,
                      REGNO  VARCHAR UNIQUE    NOT NULL,
                      CAMPUS         TEXT      NOT NULL,
                      YEAROFSTUDY    INT       NOT NULL
                  ); '''
                  , host=site_kabete['host'])

    # TODO: Create A Mass Insert Function

    insert_records_query(records_to_insert=students_records, query="""
            INSERT INTO students_kabete (
            ID, REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """, host=site_kabete['host'])


# create_fragment_kabete()

def create_master_students_table():
    execute_query(query=
               '''CREATE TABLE students2
                         (
                           ID SERIAL,
                           REGNO   VARCHAR(255) NOT NULL,
                           CAMPUS         TEXT      NOT NULL,
                           YEAROFSTUDY    INT       NOT NULL
                         ); '''
                  )


# create_master_students_table()

def fetch_fragment_chiromo():
    execute_query(query="select * from students_chiromo", host=site_chiromo['host'], )

    # FETCH PARAMETARIZED DATA

    execute_query(query="select * from students_chiromo where id = 1", host=site_chiromo['host'], )


# fetch_fragment_chiromo()

def fetch_fragment_kabete():
    print(DATABASES['site_kabete'])
    # route_query(query="select * from students_kabete", port=DATABASES['site_kabete']['port'], )
    # execute_query(query="select * from students_kabete where id = 1", host=site_kabete['host'], )


fetch_fragment_kabete()


def fetch_master_data():
    execute_query(query="select * from students")
    execute_query(query="select * from students where id = 2")


# fetch_master_data()

def delete_data_from_master():
    execute_query(query="Delete from students where id = 10")


# delete_data_from_master()

def insert_to_master():
    insert_records_query(records_to_insert=records, query="""
            INSERT INTO students (
            REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """)


# insert_to_master()

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
