import psycopg2


def update_student_record(student_id, fee_balance):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host="34.70.144.81",
                                      port="5432",
                                      database="school")

        cursor = connection.cursor()

        print("Table Before updating record ")
        sql_select_query = """select * from students where id = %s"""
        cursor.execute(sql_select_query, (student_id, ))
        record = cursor.fetchone()
        print(record)

        # Update single record now
        sql_update_query = """
        Update students set feebalance = %s where id = %s"""
        cursor.execute(sql_update_query, (fee_balance, student_id))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record Updated successfully ")

        print("Table After updating record ")
        sql_select_query = """select * from students where id = %s"""
        cursor.execute(sql_select_query, (student_id,))
        record = cursor.fetchone()
        print(record)

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


student_id = 1
fee_balance = 2000
update_student_record(student_id, fee_balance)
