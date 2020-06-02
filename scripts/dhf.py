# FRAGMENT STUDENTS

import mysql.connector
import psycopg2

from settings import DATABASES

site_chiromo = DATABASES['site_chiromo']
site_kabete = DATABASES['site_kabete']
master_students_db = DATABASES['master_students_db']


def dhf_fees_table(college):
    """Perform a DHF on fees table."""
    students = []
    try:
        connection = psycopg2.connect(user="admin",
                                      password="admin",
                                      host=master_students_db['host'],
                                      port="5432",
                                      database="school")

        print("Fetching records from STUDENTS relation")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select students.REGNO from students INNER JOIN feess_123456789 ON feess_123456789.REGNO = students.regno where students.CAMPUS = %s"

        cursor.execute(postgreSQL_select_Query, (college,))
        students_records = cursor.fetchall()
        print(f"{college} students successfully retrieved")
        for row in students_records:
            print(row[0], row[1], row[2], row[3])
            students.append(row)
        return students

    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from STUDENTS table", error)

    finally:
        # closing database connection
        if (connection):
            cursor.close()
            connection.close()
            print("Records fetched successfully relation \n")


fee_kabete = dhf_fees_table('KABETE')
fee_chiromo = dhf_fees_table('CHIROMO')


def create_dhf_fragment_table(site):
    """Create a dhf table in MySQL db on site."""
    try:
        connection = mysql.connector.connect(
            user="admin",
            password="admin",
            host=site['host'],
            database="school"
                                    )

        if connection.is_connected():
            db_Info = connection.get_server_info()
            print(f"Connected to db {site}, MySQL Server version ", db_Info)
        cursor = connection.cursor()

        create_table_query = '''CREATE TABLE dhf_fee_students_123456
            (
                ID     INT            NOT NULL UNIQUE,
                REGNO  VARCHAR(20)    NOT NULL UNIQUE,
                FEE_BALANCE         REAL
            ); '''

        result = cursor.execute(create_table_query)
        connection.commit()
        print(f"Fragment created successfully in {site['application_wide_name']}")

        return result

    except mysql.connector.Error as error:
        print("Failed to create fragment table in MySQL: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"{site['application_wide_name']} MySQL connection is closed")


# create_dhf_fragment_table(site_kabete)
# create_dhf_fragment_table(site_chiromo)


def insert_dhf_records(records, site):
    try:
        connection = mysql.connector.connect(user="admin",
                                      password="admin",
                                      host=site['host'],
                                      database="school")
        cursor = connection.cursor()

        mysql_insert_query = """
        INSERT INTO dhf_fee_students_123456 (
        REGNO, FEE_BALANCE
        ) VALUES (%s, %s)
        """

        cursor.executemany(mysql_insert_query, records)
        connection.commit()
        print(cursor.rowcount, f"Records inserted successfully into {site['application_wide_name']}")

    except mysql.connector.Error as error:
        print("Failed to insert record into {} MySQL table {}".format(site['application_wide_name'], error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print(f"CONNECTION TO FRAGMENT {site} CLOSED")


# fee_kabete = dhf_fees_table('KABETE')
# fee_chiromo = dhf_fees_table('CHIROMO')

# insert_dhf_records(fee_kabete, site_kabete)
# insert_dhf_records(fee_chiromo, site_chiromo)
