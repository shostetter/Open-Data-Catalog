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
        query(db_conn, f"DROP TABLE IF EXISTS {schema}.{table} CASCADE")
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

def create_mat_view(db_conn, overwrite=True):
    """
    Creates materialized view of census status, based on the analysis questions provided.
    If additional analyses are required this may need to be altered
    :param db_conn: daatbase connection
    :param overwrite: flag to overwrite existing mat view if it exists
    :return:
    """
    if overwrite:
        query(db_conn, "DROP MATERIALIZED VIEW IF EXISTS census_stats CASCADE;")
    qry = """
    CREATE MATERIALIZED VIEW census_stats AS
    SELECT 
        c.cpro::int as prov_id, 
        id_ine::int muni_id, 
        m.rotulo as muni_name,
        sum(c.t1_1::int) as total_pop, 
        sum(t12_5::int) as ed_uni_pop,
        st_area_sh, -- assume this is square hectares 
        -- not sure this is right but gets close to sq km numbers found on wikipedia
        st_area(wkb_geometry)*10000 as sq_m, 
        wkb_geometry as geom
    FROM public.census_data_2011 c
    JOIN public.muni m
        ON c.cpro::varchar||lpad(cmun::varchar, 3, '0')=m.id_ine
    GROUP BY  
        id_ine, 
        c.cpro, 
        m.rotulo, 
        st_area_sh, 
        wkb_geometry;

    CREATE UNIQUE INDEX census_stats_muni_idx
      ON census_stats (muni_id);
    """
    query(db_conn, qry)


def questions(db_conn):
    """
    Queries for the analysis questions provided
    :param db_conn: database connection
    :return:
    """
    # - Get the population density of each of the municipalities of Madrid
    q1 = """
        SELECT 
            muni_id, 
            muni_name, 
            total_pop, 
            st_area_sh, 
            total_pop/st_area_sh::float as pop_density 
        FROM census_stats
        WHERE prov_id = 28 
    """
    q1_results = query(db_conn, q1)

    # - Get the names of the 10 provinces with the highest percentage of
    # people with university degrees (third-level studies)
    q2 = """
            SELECT 
                prov_id, 
                sum(ed_uni_pop) university_pop, 
                sum(total_pop) total_pop, 
                100*(sum(ed_uni_pop)/sum(total_pop)::float) university_pct,
                rank() over (order by sum(ed_uni_pop)/sum(total_pop)::float desc) university_pop_rank
            FROM census_stats
            GROUP by prov_id
            LIMIT 10
        """
    q2_results = query(db_conn, q2)
    return q1_results, q2_results

#######################################################################
#  Main run code
#######################################################################

def main():
    """
    Runs for codeset
    :return: None
    """

    urls = [r'http://www.ine.es/censos2011_datos/indicadores_seccen_rejilla.xls',
            r'http://www.ine.es/censos2011_datos/indicadores_seccion_censal_csv.zip',
            r'http://centrodedescargas.cnig.es/CentroDescargas/descargaDir?secDescDirLA=114023&pagActual=1&numTotReg=5&codSerieSel=CAANE']

    db_conn = db_connect()
    for url in urls[:-1]:
        download_file(url)
    download_file(urls[-1], 'geo.zip')

    import_csvs_from_zip(db_conn, 'public', 'census_data_2011',
                         os.path.join(DOWNLOAD_PATH, 'indicadores_seccion_censal_csv.zip'))

    import_shapefile(os.path.join(DOWNLOAD_PATH, 'geo.zip/SIANE_CARTO_BASE_S_3M/anual/20110101/se89_3_admin_muni_a_x.shp'),
                     schema='public', table='muni', zip=True)
    create_mat_view(db_conn)
    q1, q2 = questions(db_conn)
    print("Population density of each of the municipalities of Madrid (limited to 10)")
    print(q1.head(10))

    print("\n10 provinces with the highest percentage of people with university degrees")
    print(q2)


if __name__ == '__main__':
    main()
