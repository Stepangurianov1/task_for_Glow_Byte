import sqlite3
from faker import Faker
import random

probability = 0.8
faker = Faker()


def create_database_tables(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS Journal (
                        record_id INTEGER PRIMARY KEY,
                        student_name TEXT NOT NULL,
                        date DATE NOT NULL,
                        mark INTEGER NOT NULL,
                        subject TEXT NOT NULL
                    )''')


#
def add_random_journal(cur, count_rows):
    list_name = [faker.name()]
    for _ in range(count_rows):
        name = faker.name()
        if probability > random.random():
            list_name.append(name)
            name = random.choice(list_name)
        date = faker.date_time_between(start_date="-1y", end_date="now")
        mark = random.choice([2, 3, 4, 5])
        subject = random.choice(
            ['Mathematics', 'English Language', 'Literature', 'History', 'Biology', 'Physics', 'Geography'])
        cur.execute('''INSERT INTO Journal (student_name, date, mark, subject)
                  VALUES (?, ?, ?, ?)''', (name, date, mark, subject))


def request_1(cur):
    request = """SELECT student_name, AVG(mark) as avg_mark FROM Journal
                 WHERE student_name NOT IN (
                      SELECT student_name
                      FROM Journal
                      WHERE mark = 2
                        AND strftime('%Y', date) = strftime('%Y', 'now')
                  )
                 GROUP BY student_name
                 HAVING avg_mark > 4.5
                    """
    cur.execute(request)
    rows = cur.fetchall()
    print(rows)


def request_2(cur):
    request = """WITH AvgGrades AS
                 (SELECT student_name, subject, AVG(mark) as avg_mark,
                 RANK() OVER(PARTITION BY student_name ORDER BY AVG(mark) DESC) as rank_high,
                 RANK() OVER(PARTITION BY student_name ORDER BY AVG(mark) ASC) as rank_low
                 FROM Journal
                 GROUP BY student_name, subject)
                 SELECT student_name, 
                 MAX(CASE WHEN rank_high = 1 THEN subject END) AS best_subject,
                 MAX(CASE WHEN rank_low = 1 THEN subject END) AS worst_subject
                 FROM AvgGrades
                 GROUP BY student_name
                 """
    cur.execute(request)
    rows = cur.fetchall()
    print(rows)


if __name__ == '__main__':
    connector = sqlite3.connect('journal.db')
    # create_database_tables(connector)
    cursor = connector.cursor()
    # add_random_journal(cursor, 100)
    request_1(cursor)
    request_2(cursor)
    connector.commit()
    connector.close()
