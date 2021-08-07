import os
import configparser
import getpass
import psycopg2
import pandas as pd
import zipfile
import subprocess
import shlex



#######################################################################
# Set up conifg file
#  Define global variables
#######################################################################
print('Checking for config file...\n\n')
if not os.path.isfile('config.cfg'):
    with open('config.cfg', 'w') as f:
        # get db connection parameters from users
        # to be stored in the config file
        # build config file

        print('Creating config file\n\n')
        f.write('[DOWNLOAD]:\n')
        print("Enter directory path to write downloaded data to.")
        data = input('download path: ')
        f.write(f'DOWNLOAD_PATH = {data}\n')

        f.write('\n\n[DB]:\n')
        print("Enter database connection details.")
        data = input('Database host: ')
        f.write(f'HOST = {data}\n')

        data = input('Database port: ')
        f.write(f'PORT = {data}\n')

        data = input('Database name: ')
        f.write(f'NAME = {data}\n')

        data = input('Database username: ')
        f.write(f'USER = {data}\n')

        data = input('Database password: ')
        f.write(f'PASSWORD = {data}\n')

config = configparser.ConfigParser()
config.read('config.cfg')

HOST = config.get('DB', 'HOST')
PORT = config.get('DB', 'PORT')
DATABASE = config.get('DB', 'NAME')
USER = config.get('DB', 'USER')
PASSWORD = config.get('DB', 'PASSWORD')

DOWNLOAD_PATH = config.get('DOWNLOAD', 'DOWNLOAD_PATH')


#######################################################################
# helper functions for working with the data base
#######################################################################
def db_connect():
    """
    Creates connection to postgres database
    :return: database connection
    """
    global USER
    global PASSWORD
    # if username / password not passed, get from user input
    if not USER:
        USER = input('User name: \n')
    if not PASSWORD:
        PASSWORD = getpass.getpass('Password: \n')
    params = {
        'dbname': DATABASE,
        'user': USER,
        'password': PASSWORD,
        'host': HOST,
        'port': PORT
    }
    conn = psycopg2.connect(**params)
    print('Connected to database')
    return conn


def query(conn, query_string):
    """
    Function to run queries in postgres.
    If query returns data (ie. select query) data is returned as pandas DataFrame
    :param conn: database connection (return from db_conect)
    :param query_string: query string to be sent to database
    :return: pandas DataFrame if data is available
    """
    # create cursor
    cur = conn.cursor()
    cur.execute(query_string)
    conn.commit()

    # check if data is available in cursor
    if cur.description:
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=columns)
    else:
        df = None
    del cur
    return df

#######################################################################
#  Load data into postgres database
#######################################################################

#######################################################################
#  Set up data for analysis questions
#######################################################################