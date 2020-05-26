# CREATE TABLE STUDENTS
import psycopg2

from functions import exec_query
from hosts import master_students_db

exec_query(query=
'''CREATE TABLE students
          (
            ID SERIAL,
            REGNO   VARCHAR   UNIQUE NOT NULL,
            CAMPUS         TEXT      NOT NULL,
            YEAROFSTUDY    INT       NOT NULL
          ); '''
        )
