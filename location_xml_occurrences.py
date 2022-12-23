import os
import xml.etree.ElementTree as ET
import logging
from setup import *
from general import *


logging.basicConfig(filename="logs/location_xml_update_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')


# Find occurrences of locations in a Publication
def find_ref(dir, fileName):
    tree = ET.parse(dir + '/' + fileName)
    processedTags = []
    addedConnections = 0
    
    # Get the ID of the Publication from the filename
    pubId = (fileName).split('_')[1]

    # Create or get the Event Id
    eventId = get_event_id(pubId)
    if eventId is None:
        eventId = create_event("publication", pubId)
    # Add Event > Publication Connection
    add_publication_occurrence(eventId, pubId, fileName)
    
    # There might be many occurrences of the same or other locations
    for data in tree.findall(".//{http://www.tei-c.org/ns/1.0}placeName[@corresp]"):
        # add only one occurrence per location per publication
        if data.attrib['corresp'] is not None and data.attrib['corresp'] not in processedTags:
            # Get the Database id for the location (using legacy_id)
            locationId = get_location_id(data.attrib['corresp'])
            if locationId is not None:
                # Connect the location to the Event. If a connection already exists, a new one is not created.
                connectionInserted = create_location_event_connection(locationId, eventId)
                processedTags.append(data.attrib['corresp'])
                if connectionInserted:
                    addedConnections += 1
                    if DEBUG is False:
                        logging.info("Added location '{}' occurrence to database for publication '{}'".format(data.attrib['corresp'], fileName))
                    else:
                        logging.info("Debug mode: location '{}' occurrence could be added to database for publication '{}'".format(data.attrib['corresp'], fileName))
            else:
                logging.warning("location missing in database '{}' in file '{}'".format(data.attrib['corresp'], fileName))
    
    return addedConnections
            
# Get ALL the publications
def find_occurrences():
    addedConnections = 0
    for root, d_names, f_names in os.walk(XML_FOLDERPATH, followlinks=True):
        file_sum = 0
        for f in f_names:
            if f.endswith(".xml"):
                file_sum += 1
        
        print(f"Processing occurrences in {file_sum} xml-files, progress:")
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
                # Check if we can find an occurrence of a location in the Publication
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
                
def main():
    print("Running script with the following arguments:")
    print(f"Project id: {PROJECT_ID}")
    print(f"Debug: {DEBUG}")
    print(f"Remove old connections: {REMOVE_OLD}")
    print()

    # Check if we should remove old connections
    if REMOVE_OLD is True:
        print("Removing old location connections from project ...")
        remove_old_location_connections()
        remove_empty_event_connections()

    addedConnections = find_occurrences()

    if DEBUG is False:
        conn_new_db.commit()
        print(f"{addedConnections} location connections added to database. Database updated.")
    else:
        print(f"{addedConnections} location connections could be added to database. Database not updated.")
    conn_new_db.close()
    
if __name__ == "__main__":
    main()
