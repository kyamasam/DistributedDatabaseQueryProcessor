# FETCH INSERTED DATA
import psycopg2

from functions import exec_query
from hosts import site_kabete

exec_query(query="select * from students_kabete", host=site_kabete,)
exec_query(query="select * from students_kabete where id = 1", host=site_kabete,)

