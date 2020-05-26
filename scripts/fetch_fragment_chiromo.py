# FETCH INSERTED DATA
import psycopg2

from functions import exec_query
from hosts import site_chiromo

exec_query(query="select * from students_chiromo", host=site_chiromo,)


# FETCH PARAMETARIZED DATA

exec_query(query="select * from students_chiromo where id = 1", host=site_chiromo,)
