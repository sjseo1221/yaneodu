import pdb

from libs.config import *
from psycopg2.pool import SimpleConnectionPool

sql_pool = None

def get_sql_connection():
    global sql_pool   #global을 안쓰면 지역변수가 되버림.

    if sql_pool is None:
        print('Initializing SQL DB connection pool')

        sql_pool = SimpleConnectionPool(
            minconn=1, maxconn=10,
            host=SQL_SERVER_ADDRESS, port=SQL_SERVER_PORT,
            dbname=SQL_DATABASE, user=SQL_USER_ID, password=SQL_USER_PW,
        )

    conn = sql_pool.getconn()

    return conn

def put_sql_connection(conn):
    global sql_pool

    sql_pool.putconn(conn)

def execute_sql_query(query):
    conn = get_sql_connection()
    cursor = conn.cursor()

    cursor.execute(query)

    rows = cursor.fetchall()
    cols = [x.name for x in cursor.description]

    #print(result)
    #print()
    #print(cursor.description) #컬럼 이름이 들어가있음

    buffer = []

    for row in rows:
        entry = { k:v for k, v in zip(cols, row) }
        buffer.append(entry)

    print(buffer)
    #pdb.set_trace()

    put_sql_connection(conn)

    return buffer
