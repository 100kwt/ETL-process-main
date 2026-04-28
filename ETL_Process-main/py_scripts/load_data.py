import pandas as pd
import psycopg2


def get_connection():
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="varr",
        user="var",
        password="123"
    )
    return connection



def load_data():
    connection = get_connection()
    cursor = connection.cursor()

    query = "SELECT * FROM cards"
    cards = pd.read_sql(query, connection)

    query = "SELECT * FROM accounts"
    accounts = pd.read_sql(query, connection)

    query = "SELECT * FROM clients"
    clients = pd.read_sql(query, connection)

    cursor.execute("""
        INSERT INTO stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
        SELECT client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt
        FROM clients
    """)


    cursor.execute("""
        INSERT INTO stg_accounts (account, valid_to, client, create_dt, update_dt)
        SELECT account, valid_to, client, create_dt, update_dt
        FROM accounts
    """)


    cursor.execute("""
        INSERT INTO stg_cards (card_num, account, create_dt, update_dt)
        SELECT card_num, account, create_dt, update_dt
        FROM cards
    """)

    connection.commit()
    cursor.close()
    connection.close()

    return cards, accounts, clients
