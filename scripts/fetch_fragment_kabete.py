# FETCH INSERTED DATA
import psycopg2

from hosts import fragment_kabete


try:
    connection = psycopg2.connect(
        user="admin",
        password="admin",
        host=fragment_kabete,
        port="5432",
        database="school")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from students_kabete"

    cursor.execute(postgreSQL_select_Query)
    print("Fetching records from FRAGMENT KABETE")
    students_records = cursor.fetchall()

    print("ID  REGNO         CAMPUS YEAR")
    for row in students_records:
        print(row[0], " ", row[1], row[2], row[3])
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
                                      host=fragment_kabete,
                                      port="5432",
                                      database="school")

        print("Fetching record from FRAGMENT KABETE")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from students_kabete where id = %s"

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
