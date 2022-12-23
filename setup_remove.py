import sys, os, re
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Set global parameters
if len(sys.argv)>1 and re.search("^[0-9]{1,3}$", sys.argv[1]):
    PROJECT_ID = sys.argv[1]
else:
    print('ERROR: PROJECT_ID needs to be given as first argument')
    exit()
    
if len(sys.argv)>2:
    if sys.argv[2] == "subjects":
        REMOVE_SUBJECT_CONNECTIONS = True
        REMOVE_LOCATION_CONNECTIONS = False
    elif sys.argv[2] == "locations":
        REMOVE_SUBJECT_CONNECTIONS = False
        REMOVE_LOCATION_CONNECTIONS = True
    else:
        REMOVE_SUBJECT_CONNECTIONS = False
        REMOVE_LOCATION_CONNECTIONS = False
else:
    REMOVE_SUBJECT_CONNECTIONS = False
    REMOVE_LOCATION_CONNECTIONS = False

if len(sys.argv)>3 and re.search("^[0-1]{1}$", sys.argv[3]):
    if int(sys.argv[3]) == 1:
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

cursor_new = conn_new_db.cursor()
