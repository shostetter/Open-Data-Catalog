import os
import configparser
import getpass
import psycopg2
import pandas as pd
import zipfile
import subprocess
import shlex
import tqdm
import requests
import shutil



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
#  get data from web
#######################################################################

def download_file(url, dest_name=None):
    """
    downloads file from url to folder
    :param url: url for data source
    :param download_path: directory where output data will be saved
    :param dest_name: file name for downloaded file, defaults to none which will then use name from path
    :return: file name of downloded data
    """

    # parse download url path for file name (if not provided)
    if not dest_name:
        dest_name = url.split('/')[-1]

    with requests.get(url, stream=True) as r:
        with open(os.path.join(DOWNLOAD_PATH, dest_name), 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    print(f'Downloaded {dest_name}...')
    return dest_name


#######################################################################
#  Load data into postgres database
#######################################################################
def set_up_table(db_conn, zip_path, schema, table, table_schema_src, overwrite=True):
    """
    Sets up empty table in the database bases on input file
    :param db_conn: database connection
    :param zip_path: path to zip file containing csv data
    :param schema: database schema to write to
    :param table: database table to write to
    :param table_schema_src:sample csv to use to construct the database table
    :param overwrite: optional flag to overwrite existing table if it exists
    :return: None
    """
    # read in csv with pandas
    # was going to get datatypes and names, but everything looks like int so just need names
    with zipfile.ZipFile(zip_path) as z:
        with z.open(table_schema_src) as f:
            df = pd.read_csv(f)

    # Parse df for schema - seems to only be float and int types, but int makes more sense and is simpler
    input_schema = list()  # list of column names and column datatypes
    for col in df.dtypes.iteritems():
        col_name, col_type = col
        input_schema.append([col_name, 'int'])

    # Create table in database
    qry = "CREATE TABLE {s}.{t} ({cols})".format(
        s=schema, t=table,
        cols=str(['"' + str(i[0]) + '" ' + i[1] for i in input_schema])[1:-1].replace("'", "")
    )

    if overwrite:
        query(db_conn, f"drop table if exists {schema}.{table}")
    print(f'table ({table}) created' )


def import_csv(file_path, dest_scehma, dest_table, zip=False, quiet=False):
    """
    Imports a csv file into database
    :param file_path: path to csv file to import
    :param dest_scehma: database schema to write to
    :param dest_table: table name to insert data into
    :param zip: boolean for if the source file is in a compressed zip file
    :return: None
    """

    if zip:
        file_path = "/vsizip/" + file_path

    cmd = f"""
        ogr2ogr -f "PostgreSQL" 
        PG:"host={HOST} 
        user={USER}
        dbname={DATABASE} 
        password={PASSWORD} 
        port={PORT}" 
        "{file_path}" 
        -oo EMPTY_STRING_AS_NULL=YES 
        -nln "{dest_scehma}.{dest_table}" 
        -append
    """
    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ').replace('\\', '/')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print("Ogr2ogr Output:\n", e.output)
    if not quiet:
        print(f'csv file {file_path} imported into database...\n')


def import_csvs_from_zip(db_conn, schema, table, file_path, sample_file="C2011_ccaa01_Indicadores.csv", overwrite=True):
    """
    Import census csv data from zip file. Assumes all csv have the same structure and naming conventions.
    This will create a new table in the database and populate it will the csv data.

    :param db_conn: database connection
    :param schema: database schema to write to
    :param table: database table to write to
    :param file_path: path to source csv file
    :param sample_file: name of csv file to use for database table schema
    :param overwrite: flag to overwrite table in database if it exists
    :return: None
    """
    print('Importing csv files into database...')
    set_up_table(db_conn, file_path, schema, table, sample_file, overwrite=overwrite)

    for i in tqdm.tqdm(range(1, 20)):
        fle = os.path.join(file_path, f'C2011_ccaa{i:02}_Indicadores.csv')
        import_csv(fle, schema, table, zip=True, quiet=True)

    df = query(db_conn, f"select count(*) cnt from {schema}.{table}")
    print(f'Added {df.cnt.values[0]} rows to {schema}.{table}\n')


def import_shapefile(shapefile, schema='public', table='muni', zip=True):
    """
    Imports shapefile to database.
    :param shapefile: path to shapefile
    :param schema: database schema to write to
    :param table: database table to write to
    :param zip: optional flag for if the shapefile is in a zip file
    :return: None
    """
    if zip:
        shapefile = "/vsizip/"+shapefile

    # If needed convert to multipolygon
    perc = '-nlt MultiPolygon -lco PRECISION=no'

    cmd = f"""ogr2ogr 
        -overwrite -progress 
        -f "PostgreSQL" 
        PG:"host=localhost 
        port={PORT} 
        dbname={DATABASE} 
        user={USER} 
        password={PASSWORD}" 
        "{shapefile}" 
        -nln {schema}.{table} 
        {perc}
    """.replace('\n', ' ')

    try:
        ogr_response = subprocess.check_output(shlex.split(cmd.replace('\n', ' ')), stderr=subprocess.STDOUT)
        print(ogr_response)
    except subprocess.CalledProcessError as e:
        print ("Ogr2ogr Output:\n", e.output)


#######################################################################
#  Set up data for analysis questions
#######################################################################