import psycopg2
import psycopg2.extras

from functions import insert_records_query


records = [
            ('P15/111/1101', 'CHIROMO', 1),
            ('P15/111/1112', 'CHIROMO', 3),
            ('P15/111/1113', 'KABETE', 1),
            ('P15/111/1114', 'CHIROMO', 4),
            ('P15/111/1115', 'KABETE', 1),
            ('P15/111/1116', 'CHIROMO', 3),
            ('P15/111/1117', 'KABETE', 1),
            ('P15/111/1118', 'CHIROMO', 4),
            ('P15/111/1119', 'KABETE', 1),
            ('P15/111/1110', 'CHIROMO', 2),
            ('P15/222/2201', 'KABETE', 1),
            ('P15/222/2202', 'CHIROMO', 3),
            ('P15/222/2203', 'KABETE', 1),
            ('P15/222/2204', 'CHIROMO', 1),
            ('P15/222/2205', 'KABETE', 2),
            ('P15/222/2206', 'CHIROMO', 1),
            ('P15/222/2207', 'KABETE', 3),
            ('P15/222/2208', 'CHIROMO', 1),
            ('P15/222/2209', 'KABETE', 4),
            ('P15/222/2210', 'KABETE', 1),
            ('P15/333/2201', 'KABETE', 1),
            ('P15/333/2202', 'CHIROMO', 3),
            ('P15/333/2203', 'KABETE', 1),
            ('P15/333/2204', 'CHIROMO', 1),
            ('P15/333/2205', 'KABETE', 2),
            ('P15/333/2206', 'CHIROMO', 1),
            ('P15/333/2207', 'KABETE', 3),
            ('P15/333/2208', 'CHIROMO', 1),
            ('P15/333/2209', 'KABETE', 4),
            ('P15/333/2210', 'KABETE', 1),
            ('P15/444/2201', 'KABETE', 1),
            ('P15/444/2202', 'CHIROMO', 3),
            ('P15/444/2203', 'KABETE', 1),
            ('P15/444/2204', 'CHIROMO', 1),
            ('P15/444/2205', 'KABETE', 2),
            ('P15/444/2206', 'CHIROMO', 1),
            ('P15/444/2207', 'KABETE', 3),
            ('P15/444/2208', 'CHIROMO', 1),
            ('P15/444/2209', 'KABETE', 4),
            ('P15/444/2210', 'KABETE', 1),
        ]


insert_records_query(records_to_insert=records, query="""
        INSERT INTO students (
        REGNO, CAMPUS, YEAROFSTUDY
        ) VALUES %s
        """)
