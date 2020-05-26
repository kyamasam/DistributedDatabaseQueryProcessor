# FRAGMENT STUDENTS

import psycopg2

from functions import exec_query, insert_records_query
from hosts import (
    site_chiromo,
    master_students_db,
)

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

