import psycopg2

from hosts import master_students_db


records = [
            ('P15/0000/1101', 'CHIROMO', 1),
            ('P15/0000/1112', 'CHIROMO', 1),
            ('P15/0000/1113', 'CHIROMO', 1),
            ('P15/0000/1114', 'CHIROMO', 1),
            ('P15/0000/1115', 'CHIROMO', 1),
            ('P15/0000/1116', 'CHIROMO', 1),
            ('P15/0000/1117', 'CHIROMO', 1),
            ('P15/0000/1118', 'CHIROMO', 1),
            ('P15/0000/1119', 'CHIROMO', 1),
            ('P15/0000/1110', 'CHIROMO', 1)
        ]


def insert_records(records):
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db,
                                      port="5432",
                                      database="school")
        cursor = connection.cursor()

        postgres_insert_query = """
        INSERT INTO students (
        REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES (%s,%s, %s)
        """
        for record in records:
            cursor.execute(postgres_insert_query, record)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into students table")

    except (Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert records into students table", error)

    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


insert_records(records)
