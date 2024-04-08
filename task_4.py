import sqlite3
from faker import Faker
import random

probability = 0.8
fake = Faker()


def create_database_tables(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS Calls (
                        id_call INTEGER PRIMARY KEY,
                        id_client INTEGER,
                        call_datetime DATETIME NOT NULL
                    )''')


def add_random_call(cur, count_rows):
    for _ in range(count_rows):
        call_datetime = fake.date_time_between(start_date='-1d', end_date='now')
        client_id = random.randint(10, 100)
        cur.execute('''INSERT INTO Calls (id_client, call_datetime)
                          VALUES (?, ?)''', (client_id, call_datetime))


def request_1(cur):
    request = """
        WITH CallsWithGaps AS (
        SELECT id_client, id_call, call_datetime,
        strftime('%s', call_datetime) - strftime('%s', lag(call_datetime) OVER (PARTITION BY id_client ORDER BY call_datetime)) 
        AS time_diff
        FROM Calls),
        CallsWithFlag AS (
        SELECT id_client, time_diff, id_call,
        (CASE WHEN time_diff < 3600 THEN 1 ELSE 0 END) as flag
        FROM CallsWithGaps)
        SELECT id_client, SUM(end_session) / 2 FROM(
        SELECT 
        CASE WHEN lag(flag) OVER (PARTITION BY id_client) = flag THEN 0
        ELSE 1 END as end_session,
        flag,
        id_client
        FROM CallsWithFlag)
        GROUP BY id_client
         """

    cur.execute(request)
    rows = cur.fetchall()
    print(rows)


if __name__ == "__main__":
    connector = sqlite3.connect('call_centers.db')
    cursor = connector.cursor()
    create_database_tables(connector)
    # add_random_call(cursor, 1000)
    request_1(cursor)
    connector.commit()
    connector.close()
