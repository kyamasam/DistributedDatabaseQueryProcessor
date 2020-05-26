import psycopg2


def delete_student_record(student_id):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="school")

        cursor = connection.cursor()

        # Update single record now
        sql_delete_query = """Delete from students where id = %s"""
        cursor.execute(sql_delete_query, (student_id, ))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully ")

    except (Exception, psycopg2.Error) as error:
        print("Error in Delete operation", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


student_id = 4
student_id = 12
delete_student_record(student_id)
delete_student_record(student_id)
