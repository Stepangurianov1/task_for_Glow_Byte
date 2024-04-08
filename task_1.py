import sqlite3
from faker import Faker
import random
import datetime

probability = 0.99


def create_database_tables(cur):
    cur.execute('''CREATE TABLE IF NOT EXISTS Books (
                    book_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT NOT NULL,
                    publisher TEXT,
                    publication_year INTEGER,
                    publication_city TEXT,
                    page_count INTEGER
                )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS BookInstances (
                    instance_id INTEGER PRIMARY KEY,
                    book_id INTEGER,
                    FOREIGN KEY (book_id) REFERENCES Books(book_id) ON DELETE CASCADE
                )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Readers (
                    reader_ticket_number INTEGER PRIMARY KEY,
                    last_name TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    patronymic TEXT,
                    date_of_birth DATE,
                    gender TEXT,
                    address TEXT,
                    phone TEXT
                )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS BookLoans (
                    instance_id INTEGER NOT NULL,
                    loan_date DATE,
                    return_date DATE,
                    reader_ticket_number INTEGER NOT NULL,
                    FOREIGN KEY (instance_id) REFERENCES BookInstances(instance_id) ON DELETE CASCADE
                    FOREIGN KEY (reader_ticket_number) REFERENCES Readers(reader_ticket_number) ON DELETE CASCADE
                )''')


def add_random_book(cur, count_rows: int):
    data_dict = {
        'author': [fake.name()],
        'publisher': [fake.company()],
        'publication_year': [],
        'publication_city': [fake.city()],
        'page_count': [fake.random_number(digits=3)]
    }
    for _ in range(count_rows):
        title = fake.sentence(nb_words=3)
        if random.random() < 0.1:
            title = 'Война и мир'
        if random.random() < probability:
            author = random.choice(data_dict['author'])
        else:
            author = fake.name()
            data_dict['author'].append(author)
        if random.random() < 0.1:
            author = 'Л.Н.Толстой'
        if random.random() < probability:
            publisher = random.choice(data_dict['publisher'])
        else:
            publisher = fake.company()
            data_dict['publisher'].append(publisher)
        publication_year = random.randint(2000, 2024)
        if random.random() < probability:
            publication_city = random.choice(data_dict['publication_city'])
        else:
            publication_city = fake.city()
            data_dict['publication_city'].append(publication_city)
        if random.random() < probability:
            page_count = random.choice(data_dict['page_count'])
        else:
            page_count = fake.random_number(digits=3)
            data_dict['page_count'].append(page_count)
        cur.execute('''INSERT INTO Books (title, author, publisher, publication_year, publication_city, page_count)
                        VALUES (?, ?, ?, ?, ?, ?)''',
                    (title, author, publisher, publication_year, publication_city, page_count))


def add_random_reader(cur, count_rows):
    data_dict = {
        'first_name': [fake.first_name()],
        'address': [fake.address()],
    }
    for _ in range(count_rows):
        last_name = fake.last_name()
        if random.random() < probability:
            first_name = random.choice(data_dict['first_name'])
        else:
            first_name = fake.first_name()
            data_dict['first_name'].append(first_name)
        patronymic = fake.first_name_male() if random.choice(
            [True, False]) else fake.first_name_female()
        date_of_birth = fake.date_of_birth(minimum_age=18, maximum_age=50)
        gender = random.choice(['Male', 'Female'])
        if random.random() < probability:
            address = random.choice(data_dict['address'])
        else:
            address = fake.address()
            data_dict['address'].append(address)
        phone = fake.phone_number()
        cur.execute('''INSERT INTO Readers (last_name, first_name, patronymic, date_of_birth, gender, address, phone)
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (last_name, first_name, patronymic, date_of_birth, gender, address, phone))


def add_random_book_instance(cur, count_rows):
    for _ in range(count_rows):
        cur.execute('''INSERT INTO BookInstances (book_id)
                        VALUES (?)''', (random.randint(500, 700),))


def add_random_book_loan(cur, count_rows):
    for _ in range(count_rows):
        instance_id = random.randint(500, 700)
        reader_ticket_number = random.randint(500, 700)
        loan_date = fake.date_time_between(start_date="-1y", end_date="now")
        if random.random() > 0.5:
            return_date = loan_date + datetime.timedelta(days=random.randint(7, 20))
        else:
            return_date = None
        cur.execute('''INSERT INTO BookLoans (instance_id, loan_date, return_date, reader_ticket_number)
                        VALUES (?, ?, ?, ?)''', (instance_id, loan_date, return_date, reader_ticket_number))


def request_1(cur):
    """
    Найти город (или города), в котором в 2016 году было издано больше всего книг (не экземпляров).
    """
    request = """SELECT publication_city, COUNT(*) as book_count
                FROM Books
                WHERE publication_year = 2016
                GROUP BY publication_city
                HAVING COUNT(*) = (
                    SELECT MAX(book_count)
                    FROM (
                        SELECT COUNT(*) as book_count
                        FROM Books
                        WHERE publication_year = 2016
                        GROUP BY publication_city
                    ) AS subquery
                );
                """
    cur.execute(request)
    rows = cur.fetchall()
    print(rows)
    print('Request 1: ')
    print(f'publication_city - {rows[0][0]}, count_book - {rows[0][1]};')
    print(100 * '-')


def request_2(cur):
    """
    Вывести количество экземпляров книг «Война и мир» Л.Н.Толстого, которые сейчас находятся в библиотеке (не на руках у читателей).
    """
    request = """SELECT COUNT(book_count), author FROM 
                 (SELECT COUNT(BookInstances.instance_id) as book_count, Books.author as author  FROM BookInstances
                 JOIN BookLoans ON BookInstances.instance_id=BookLoans.instance_id
                 JOIN Books ON BookInstances.book_id=Books.book_id
                 WHERE BookLoans.return_date is NOT NULL AND
                 Books.author = 'Л.Н.Толстой' AND
                 Books.title = 'Война и мир'
                 GROUP BY BookInstances.instance_id)
                 """
    cur.execute(request)
    rows = cur.fetchall()
    print(rows)
    print('Request 2: ')
    print(f'count_instance - {rows[0][0]}, author - {rows[0][1]};')
    print(100 * '-')


def request_3(cur):
    """
    Найти читателя, который за последний месяц брал больше всего книг в библиотеке.
    Если читателей с максимальным количество несколько - вывести только тех, у кого самый маленький возраст.
    """
    request = """SELECT name, last_name,  MAX(date_of_birth) FROM 
                (SELECT Readers.first_name as name, COUNT(*) as count_loan, Readers.date_of_birth, Readers.last_name as last_name FROM Readers
                 JOIN BookLoans ON Readers.reader_ticket_number = BookLoans.reader_ticket_number
                 WHERE BookLoans.loan_date >= DATE('now', '-1 month')
                 GROUP BY Readers.reader_ticket_number 
                 HAVING count_loan = (
                    SELECT MAX(count_loan)
                    FROM (
                        SELECT Readers.first_name, COUNT(*) as count_loan FROM Readers JOIN BookLoans
                         ON Readers.reader_ticket_number = BookLoans.reader_ticket_number
                         WHERE BookLoans.loan_date >= DATE('now', '-1 month')
                         GROUP BY Readers.reader_ticket_number
                    )
                ))
                 """
    cur.execute(request)
    rows = cur.fetchall()
    print('Request 3: ')
    print(f'first_name - {rows[0][0]}, last_name - {rows[0][1]}, date_of_birth - {rows[0][2]};')
    print(100 * '-')


if __name__ == "__main__":
    fake = Faker()
    connector = sqlite3.connect('library.db')
    create_database_tables(connector)
    cursor = connector.cursor()
    add_random_book(cursor, 10000)
    add_random_reader(cursor, 10000)
    add_random_book_instance(cursor, 10000)
    add_random_book_loan(cursor, 10000)
    request_1(cursor)

    request_2(cursor)
    request_3(cursor)
    connector.commit()
    connector.close()
