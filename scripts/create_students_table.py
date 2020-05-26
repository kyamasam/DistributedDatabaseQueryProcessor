# CREATE TABLE STUDENTS
import psycopg2
try:
    connection = psycopg2.connect(
        user="admin",
        password="admin",
        host="34.70.144.81",
        port="5432",
        database="school"
                                  )

    cursor = connection.cursor()

    create_table_query = '''CREATE TABLE students
          (
            ID SERIAL,
            REGNO          VARCHAR   NOT NULL,
            CAMPUS         TEXT      NOT NULL,
            YEAROFSTUDY    INT       NOT NULL
          ); '''

    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while creating PostgreSQL table", error)
finally:
    # closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
