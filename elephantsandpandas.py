import pandas as pd
import psycopg2

from utils import get_config_field

conn = None


def db_connect():
    print("Opening new db connection")
    global conn
    account = get_config_field('POSTGRESDB', 'pg_account')
    passwd = get_config_field('POSTGRESDB', 'pg_password')
    host = get_config_field('POSTGRESDB', 'pg_host')
    db_name = get_config_field('POSTGRESDB', 'pg_db_name')

    conn = psycopg2.connect(f"host='{host}' dbname='{db_name}' user={account} password={passwd}")


def db_query(sql):
    if not conn:
        db_connect()
    data = pd.read_sql_query(sql, conn)
    return data


def sql_file_query(sql_file):
    with open(sql_file, 'r') as sql_fd:
        sql = sql_fd.read()
    return db_query(sql)
