import os
import xml.etree.ElementTree as ET
import logging
from setup import *
from general import *


logging.basicConfig(filename="subject_xml_update_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')


# Find occurrences of subjects in a Publication
def findRef(dir, fileName):
    tree = ET.parse(dir + '/' + fileName)
    addedTags = []
    
    # Get the ID of the Publication from the filename
    pubId = (fileName).split('_')[1]
    # Create or get the Event Id
    eventId = getEventId(pubId)
    if eventId is None:
        eventId = createEvent(pubId)
    # Add Event > Publication Connection
    addPublictionOccurrence(eventId, pubId, fileName)
    
    # There might be many occurrences of the same or other subjects
    for data in tree.findall(".//{http://www.tei-c.org/ns/1.0}persName[@corresp]"):
        # add only one occurrence per subject per publication
        if data.attrib['corresp'] is not None and data.attrib['corresp'] not in addedTags:
            # Get the Database id for the Subject (using legacy_id)
            subjectId = getSubjectId(data.attrib['corresp'])
            if subjectId is not None:
                # Connect the Subject to the Event           
                createSubjectEventConnection(subjectId, eventId)
                addedTags.append(data.attrib['corresp'])
                logging.info("Added subject '{}' occurrence to database for publication '{}'".format(data.attrib['corresp'], fileName))
            else:
                logging.warn("Subject missing in database '{}' in file '{}'".format(data.attrib['corresp'], fileName))


# Get ALL the publications
def findOccurrences():
    for root, d_names, f_names in os.walk(XML_PATH, followlinks=True):
        for f in f_names:
            if f.endswith(".xml"):
                # Check if we can find an occurrence of a subject in the Publication
                findRef(root, f)
                # Commit often, allot of data...
                if DEBUG is False:
                    conn_new_db.commit()

def main():
    # Check if we should remove old connections
    if REMOVE_OLD is True:
        removeOldSubjectConnections();
        removeEmptyEventConnections();
                    
    findOccurrences()

    if DEBUG is False:
        conn_new_db.commit()
    conn_new_db.close()
    
if __name__ == "__main__":
    main()