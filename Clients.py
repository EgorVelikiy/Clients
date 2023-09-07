import psycopg2
from psycopg2 import sql
from psycopg2.sql import Identifier, SQL


def create_db(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Client(
            id SERIAL PRIMARY KEY,
            name VARCHAR(40) NOT NULL,
            surname VARCHAR(40) NOT NULL,
            mail VARCHAR(80) UNIQUE NOT NULL );
        """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Phones(
            id SERIAL PRIMARY KEY,
            phone VARCHAR(11) UNIQUE NOT NULL,
            client_id INTEGER NOT NULL REFERENCES Client(id) );
        """)
    conn.commit()


def add_client(cur, first_name, last_name, email, phones=None):
    cur.execute("""
        INSERT INTO Client(name, surname, mail) 
        VALUES (%s, %s, %s)
        RETURNING id, name, surname, mail;
        """, (first_name, last_name, email))
    return cur.fetchone()


def add_phone_number(cur, client_id, phone_numb):
    cur.execute("""
        INSERT INTO Phones(phone, client_id)
        VALUES (%s, %s)
        RETURNING id, phone, client_id;
        """, (phone_numb, client_id))
    return cur.fetchall()


def change_info(cur, id, first_name=None, last_name=None, email=None, phone=None):
    if first_name is not None:
        cur.execute("""
            UPDATE Client SET name = %s
            WHERE id = %s RETURNING id;
            """, (first_name, id))
    if last_name is not None:
        cur.execute("""
            UPDATE Client SET surname = %s
            WHERE id = %s RETURNING id;
            """, (last_name, id))
    if email is not None:
        cur.execute("""
            UPDATE Client SET mail = %s
            WHERE id = %s RETURNING id;
            """, (email, id))
    if phone is not None:
        cur.execute("""
            UPDATE Phones SET number=%s
            WHERE client_id = %s RETURNING id;
            """, (phone, id))
    return cur.fetchall()


def delete_phone(conn, client_id, phone):
    cur.execute("""
        DELETE FROM Phones
        WHERE client_id=%s and phone = %s;
        """, (client_id, phone))
    conn.commit()


def delete_client(conn, client_id):
    cur.execute("""
        DELETE FROM Phones WHERE client_id=%s;
        """, (client_id,))
    cur.execute("""
        DELETE FROM Client WHERE id=%s;
        """, (client_id,))
    conn.commit()


def find_client(cur, first_name=None, last_name=None, email=None):
    arg_list = {'name': first_name, "surname": last_name, 'mail': email}
    for key, arg in arg_list.items():
        if arg:
            cur.execute(SQL("SELECT * FROM Client WHERE {k} = %s").format(k=sql.Identifier(key)), (arg,))
    return cur.fetchone()


def view_table_client(cur):
    cur.execute("""
        SELECT * FROM Client;
        """)
    return cur.fetchall()


def view_table_phones(cur):
    cur.execute("""
        SELECT * FROM Phones;
        """)
    return cur.fetchall()


with psycopg2.connect(database="clients_db", user="postgres", password="456egor4ik456", host="localhost") as conn:
    with conn.cursor() as cur:
        # print(add_client(cur, 'Ivan', 'Ivanov', 'Ivan@mail.ru', ''))
        # print(add_client(cur, 'Petr', 'Petrov', 'Petya2000@mail.ru', ''))
        # print(add_client(cur, 'Vova', 'AAAA', 'VOVAn@mail.ru', ''))
        # print(add_phone_number(cur, 8, '89889380808'))
        # print(add_phone_number(cur, 8, '89889380800'))
        # print(change_info(cur, 20, first_name='Vova', last_name='BBB', email=None, phone=None))
        # print(delete_phone(conn, 8, '89889380800'))
        # print(delete_client(conn, 8))
        # print(find_client(cur, last_name='BBB'))
        # print(view_table_client(cur))
        # print(view_table_phones(cur))
