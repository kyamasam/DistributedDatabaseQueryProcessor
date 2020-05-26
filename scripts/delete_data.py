import psycopg2

from functions import exec_query

exec_query(query="Delete from students where id = 7")
