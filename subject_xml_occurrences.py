import os
import xml.etree.ElementTree as ET
import logging
from setup import *
from general import *


logging.basicConfig(filename="logs/subject_xml_update_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')


# Find occurrences of subjects in a Publication
def find_ref(dir, fileName):
    tree = ET.parse(dir + '/' + fileName)
    processedTags = []
    addedConnections = 0
    
    # Get the ID of the Publication from the filename
    pubId = (fileName).split('_')[1]

    # Create or get the Event Id
    # eventId = get_event_id_for_subject_connection(pubId)
    # if eventId is None:
    eventId = get_event_id(pubId)
    if eventId is None:
        eventId = create_event("publication", pubId)
    # Add Event > Publication Connection
    add_publication_occurrence(eventId, pubId, fileName)
    
    # There might be many occurrences of the same or other subjects
    for data in tree.findall(".//{http://www.tei-c.org/ns/1.0}persName[@corresp]"):
        # add only one occurrence per subject per publication
        if data.attrib['corresp'] is not None and data.attrib['corresp'] not in processedTags:
            # Get the Database id for the Subject (using legacy_id)
            subjectId = get_subject_id(data.attrib['corresp'])
            if subjectId is not None:
                # Connect the Subject to the Event. If a connection already exists, a new one is not created.
                connectionInserted = create_subject_event_connection(subjectId, eventId)
                processedTags.append(data.attrib['corresp'])
                if connectionInserted:
                    addedConnections += 1
                    if DEBUG is False:
                        logging.info("Added subject '{}' occurrence to database for publication '{}'".format(data.attrib['corresp'], fileName))
                    else:
                        logging.info("Debug mode: subject '{}' occurrence could be added to database for publication '{}'".format(data.attrib['corresp'], fileName))
            else:
                logging.warning("Subject missing in database '{}' in file '{}'".format(data.attrib['corresp'], fileName))

    return addedConnections


# Get ALL the publications
def find_occurrences():
    addedConnections = 0
    for root, d_names, f_names in os.walk(XML_FOLDERPATH, followlinks=True):
        file_sum = 0
        for f in f_names:
            if f.endswith(".xml"):
                file_sum += 1
        
        print(f"Processing subject occurrences in {file_sum} xml-files, progress:")
        print("0         50        100 %")
        print("|", end="", flush=True)
        
        file_counter = 0
        progress = 1
        progress_divisor = 20
        for f in f_names:
            if f.endswith(".xml"):
                # Print progress to screen
                if file_sum > progress_divisor:
                    if file_counter > (((file_sum-1) / progress_divisor) * progress):
                        print("-", end="", flush=True)
                        progress += 1
                # Check if we can find an occurrence of a subject in the Publication
                addedConnections += find_ref(root, f)
                # Commit often, allot of data...
                if DEBUG is False:
                    conn_new_db.commit()
                
                file_counter += 1

        if file_sum > progress_divisor:
            print("-|")
        else:
            print("--------------------|")
        print()

    return addedConnections


# Go through xml-files and remove subject connections from database that are not present in the files
def remove_removed_subject_connections():
    removedConnectionsCounter = 0

    for root, d_names, f_names in os.walk(XML_FOLDERPATH, followlinks=True):
        file_sum = 0
        for f in f_names:
            if f.endswith(".xml"):
                file_sum += 1
        
        print(f"Removing old subject connections from database based on {file_sum} xml-files, progress:")
        print("0         50        100 %")
        print("|", end="", flush=True)
        
        file_counter = 0
        progress = 1
        progress_divisor = 20
        for f in f_names:
            if f.endswith(".xml"):
                # Print progress to screen
                if file_sum > progress_divisor:
                    if file_counter > (((file_sum-1) / progress_divisor) * progress):
                        print("-", end="", flush=True)
                        progress += 1

                # Get all unique subject ids in the xml-file
                subjectIdsInFile = find_unique_subjects_in_file(root, f)

                # Get the ID of the Publication from the filename
                pubId = f.split('_')[1]

                # Get all subjectIds connected to the publication in the database
                subjectConnections = get_all_subject_event_connections_for_publication(pubId)

                # Loop through all subject connections for the publication found in the database and
                # remove connection if the subject id is not found among the ids in the file
                for connection in subjectConnections:
                    if connection[1] not in subjectIdsInFile:
                        removedConnectionId = remove_event_connection("subject", connection[0])
                        removedConnectionsCounter += 1
                        if DEBUG is False:
                            conn_new_db.commit()
                            logging.info("Removed connection in publication '{}': id '{}' subject_id '{}'".format(pubId, removedConnectionId, connection[1]))
                        else:
                            logging.info("Connection could be removed in publication '{}': id '{}' subject_id '{}'".format(pubId, removedConnectionId, connection[1]))
                
                file_counter += 1
        if file_sum > progress_divisor:
            print("-|")
        else:
            print("--------------------|")
        print()
    
    remove_empty_event_connections()
    if DEBUG is False:
        conn_new_db.commit()
    return removedConnectionsCounter


def find_unique_subjects_in_file(dir, fileName):
    tree = ET.parse(dir + '/' + fileName)
    ids = []
    processedTags = []

    for data in tree.findall(".//{http://www.tei-c.org/ns/1.0}persName[@corresp]"):
        if data.attrib['corresp'] is not None and data.attrib['corresp'] not in processedTags:
            processedTags.append(data.attrib['corresp'])
            # Get the Database id for the Subject (using legacy_id)
            subjectId = get_subject_id(data.attrib['corresp'])
            ids.append(subjectId)
    return ids


def main():
    print()
    print("Running script with the following arguments:")
    print(f"Project id: {PROJECT_ID}")
    print(f"Debug: {DEBUG}")
    print(f"Remove old connections: {REMOVE_OLD}", end="\n\n")

    # Check if we should remove old connections
    if REMOVE_OLD is True:
        removedConnections = remove_removed_subject_connections()
        if DEBUG is False:
            print(f"Removed {removedConnections} connections.", end="\n\n")
        else:
            print(f"Found {removedConnections} connections that should be removed.", end="\n\n")

    addedConnections = find_occurrences()

    if DEBUG is False:
        conn_new_db.commit()
        print(f"{addedConnections} subject connections added to database. Database updated.")
    else:
        print(f"{addedConnections} subject connections could be added to database. Database not updated.")
    print()
    conn_new_db.close()
    
if __name__ == "__main__":
    main()
