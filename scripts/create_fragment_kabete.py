# FRAGMENT STUDENTS

import psycopg2

from functions import exec_query, insert_records_query
from hosts import (
    site_chiromo,
    master_students_db, site_kabete,
)

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
