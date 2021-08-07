import os
import configparser



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


#######################################################################
#  Load data into postgres database
#######################################################################


#######################################################################
#  Set up data for analysis questions
#######################################################################