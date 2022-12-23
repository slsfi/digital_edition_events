import sys
import os
import re
import psycopg2
import psycopg2.extras
import csv
from dotenv import load_dotenv

load_dotenv()

# Set global parameters
if len(sys.argv) > 1 and re.search("^[0-1]{1}$", sys.argv[1]):
    if int(sys.argv[1]) == 1:
        DEBUG = True
    else:
        DEBUG = False
else:
    DEBUG = True
    
if os.getenv("DB_USER") is None:
    print('ERROR: DB_USER needs to be given in .env file')
    exit()

if os.getenv("DB_PASSWORD") is None:
    print('ERROR: DB_PASSWORD needs to be given in .env file')
    exit()

if os.getenv("DB_DATABASE") is None:
    print('ERROR: DB_DATABASE needs to be given in .env file')
    exit()

if os.getenv("DB_PORT") is None:
    print('ERROR: DB_PORT needs to be given in .env file')
    exit()
    
if os.getenv("DB_HOST") is None:
    print('ERROR: DB_HOST needs to be given in .env file')
    exit()

conn_new_db = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_DATABASE"),
    user=os.getenv("DB_USER"),
    port=os.getenv("DB_PORT"),
    password=os.getenv("DB_PASSWORD")
)

cursor_new = conn_new_db.cursor(cursor_factory = psycopg2.extras.DictCursor)

PROJECT_ID = os.getenv("PROJECT_ID")
XML_FOLDERPATH = os.getenv("XML_FOLDERPATH")
INPUT_FILEPATH = os.getenv("INPUT_FILEPATH")
TAG_LEGACY_ID_PREFIX = os.getenv("TAG_LEGACY_ID_PREFIX")

CSV_DELIMITER = os.getenv("CSV_DELIMITER")
CSV_QUOTECHAR = os.getenv("CSV_QUOTECHAR")

if os.getenv("CSV_DOUBLEQUOTE") == "False":
    CSV_DOUBLEQUOTE = False
else:
    CSV_DOUBLEQUOTE = True

if os.getenv("CSV_ESCAPECHAR") == "":
    CSV_ESCAPECHAR = None
else:
    CSV_ESCAPECHAR = os.getenv("CSV_ESCAPECHAR")

if os.getenv("CSV_QUOTING") == "csv.QUOTE_MINIMAL":
    CSV_QUOTING = csv.QUOTE_MINIMAL
elif os.getenv("CSV_QUOTING") == "csv.QUOTE_ALL":
    CSV_QUOTING = csv.QUOTE_ALL
elif os.getenv("CSV_QUOTING") == "csv.QUOTE_NONNUMERIC":
    CSV_QUOTING = csv.QUOTE_NONNUMERIC
else:
    CSV_QUOTING = csv.QUOTE_NONE


def require_csv_parameters_in_env_file():
    if os.getenv("CSV_DELIMITER") is None:
        print('ERROR: CSV_DELIMITER needs to be given in .env file')
        exit()
    if os.getenv("CSV_QUOTECHAR") is None:
        print('ERROR: CSV_QUOTECHAR needs to be given in .env file')
        exit()
    if os.getenv("CSV_DOUBLEQUOTE") is None:
        print('ERROR: CSV_DOUBLEQUOTE needs to be given in .env file')
        exit()
    if os.getenv("CSV_QUOTING") is None:
        print('ERROR: CSV_QUOTING needs to be given in .env file')
        exit()


def require_input_filepath_in_env_file():
    if os.getenv("INPUT_FILEPATH") is None:
        print('ERROR: INPUT_FILEPATH needs to be given in .env file')
        exit()


def require_xml_folderpath_in_env_file():
    if os.getenv("XML_FOLDERPATH") is None:
        print('ERROR: XML_FOLDERPATH needs to be given in .env file')
        exit()


def require_project_id_in_env_file():
    if os.getenv("PROJECT_ID") is None:
        print('ERROR: PROJECT_ID needs to be given in .env file')
        exit()


def require_tag_legacy_id_prefix_in_env_file():
    if os.getenv("TAG_LEGACY_ID_PREFIX") is None:
        print('ERROR: TAG_LEGACY_ID_PREFIX needs to be given in .env file')
        exit()