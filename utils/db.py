import mysql.connector
from mysql.connector import Error
from config import db_config

def get_db_connection(dictionary=False):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=dictionary)
        return connection, cursor
    except Error as e:
        raise e