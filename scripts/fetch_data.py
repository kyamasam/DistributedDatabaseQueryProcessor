# FETCH INSERTED DATA
import psycopg2
try:
    connection = psycopg2.connect(
        user="admin",
        password="admin",
        host="34.70.144.81",
        port="5432",
        database="school")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from students"

    cursor.execute(postgreSQL_select_Query)
    print("Selecting rows from students table using cursor.fetchall")
    students_records = cursor.fetchall()

    print("Print each row and it's columns values")
    for row in students_records:
        print("Id = ", row[0],)
        print("Reg = ", row[1])
        print("Campus  = ", row[2])
        print("YearOfStudy  = ", row[3])
except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


# FETCH PARAMETARIZED DATA
def get_student_details(student_id):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host="34.70.144.81",
                                      port="5432",
                                      database="school")

        print("Using Python variable in PostgreSQL select Query")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from students where id = %s"

        cursor.execute(postgreSQL_select_Query, (student_id,))
        students_records = cursor.fetchall()
        for row in students_records:
            print("Id = ", row[0], )
            print("Reg = ", row[1])
            print("Campus  = ", row[2])
            print("YearOfStudy  = ", row[3])

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed \n")


# get_student_details(2)
