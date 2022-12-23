from setup_remove import *
from general import *


def main():
    print("Running script with the following arguments:")
    print(f"Project id: {PROJECT_ID}")
    print(f"Remove old subject connections: {REMOVE_SUBJECT_CONNECTIONS}")
    print(f"Remove old location connections: {REMOVE_LOCATION_CONNECTIONS}")
    print(f"Debug: {DEBUG}", end="\n\n")

    if REMOVE_SUBJECT_CONNECTIONS is True:
        print("Removing old subject connections from project ...")
        remove_old_subject_connections()
        remove_empty_event_connections()

    if REMOVE_LOCATION_CONNECTIONS is True:
        print("Removing old location connections from project ...")
        remove_old_location_connections()
        remove_empty_event_connections()

    if DEBUG is False:
        conn_new_db.commit()
        print(f"Connections removed. Database updated.")
    else:
        print(f"Debug mode: database not updated.")
    conn_new_db.close()
    
if __name__ == "__main__":
    main()
