import psycopg2

from data.students_records import records
from hosts import (
    site_chiromo,
    master_students_db, site_kabete,
)
from postgres.postgres_functions import exec_query, insert_records_query


def create_fragment_chiromo():
    students = []
    students_records = exec_query(query="select * from students where campus = 'CHIROMO'", host=master_students_db)
    exec_query(query=
               '''CREATE TABLE students_chiromo
                  (
                      ID     INT     UNIQUE    NOT NULL,
                      REGNO  VARCHAR UNIQUE    NOT NULL,
                      CAMPUS         TEXT      NOT NULL,
                      YEAROFSTUDY    INT       NOT NULL
                  ); '''
               , host=site_chiromo)

    insert_records_query(records_to_insert=students_records, query="""
            INSERT INTO students_chiromo (
            ID, REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """, host=site_chiromo)


# create_fragment_chiromo


def create_fragment_kabete():
    students = []
    students_records = exec_query(query="select * from students where campus = 'KABETE'", host=master_students_db)
    exec_query(query=
               '''CREATE TABLE students_kabete
                  (
                      ID     INT     UNIQUE    NOT NULL,
                      REGNO  VARCHAR UNIQUE    NOT NULL,
                      CAMPUS         TEXT      NOT NULL,
                      YEAROFSTUDY    INT       NOT NULL
                  ); '''
               , host=site_kabete)

    # TODO: Create A Mass Insert Function

    insert_records_query(records_to_insert=students_records, query="""
            INSERT INTO students_kabete (
            ID, REGNO, CAMPUS, YEAROFSTUDY
            ) VALUES %s
            """, host=site_kabete)


# create_fragment_kabete()

def create_master_students_table():
    exec_query(query=
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
    exec_query(query="select * from students_chiromo", host=site_chiromo, )

    # FETCH PARAMETARIZED DATA

    exec_query(query="select * from students_chiromo where id = 1", host=site_chiromo, )


# fetch_fragment_chiromo()

def fetch_fragment_kabete():
    exec_query(query="select * from students_kabete", host=site_kabete, )
    exec_query(query="select * from students_kabete where id = 1", host=site_kabete, )


# fetch_fragment_kabete()


def fetch_master_data():
    exec_query(query="select * from students")
    exec_query(query="select * from students where id = 2")


# fetch_master_data()

def delete_data_from_master():
    exec_query(query="Delete from students where id = 7")


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
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db,
                                      port="5432",
                                      database="school")

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

# update_student_record(1, 'Chiromo')
