# FETCH INSERTED DATA
from functions import exec_query

exec_query(query="select * from students")
exec_query(query="select * from students where id = 2")
