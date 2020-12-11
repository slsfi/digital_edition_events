from setup import *

def removeOldSubjectConnections():
    e_update_sql = """ UPDATE event_connection SET subject_id = NULL, date_modified=now() WHERE subject_id IN ( select id from subject where project_id = %s and type = 'person' ) AND work_manifestation_id is NULL AND type='publication-subject-occurrence' """
    values_to_find = (PROJECT_ID, )
    cursor_new.execute(e_update_sql, values_to_find)
    conn_new_db.commit()


def removeOldLocationConnections():
    e_update_sql = """ UPDATE event_connection SET location_id = NULL, date_modified=now() WHERE location_id IN ( select id from location where project_id = %s ) AND work_manifestation_id is NULL AND type='publication-location-occurrence' """
    values_to_find = (PROJECT_ID, )
    cursor_new.execute(e_update_sql, values_to_find)
    conn_new_db.commit()
    

def removeEmptyEventConnections():
    e_delete_sql = """ UPDATE event_connection SET deleted = 1, date_modified = now() WHERE subject_id is NULL AND tag_id IS NULL AND location_id IS NULL AND work_manifestation_id is NULL AND correspondence_id IS NULL """
    cursor_new.execute(e_delete_sql)
    conn_new_db.commit()


def getEventId(publicationId):
    find_sql = """ SELECT id FROM event WHERE id in (select event_id from event_occurrence where publication_id = %s and deleted !=1) AND deleted != 1 """
    values_to_find = (publicationId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


def getLocationEventConnectionId(eventId, locationId):
    find_sql = """ SELECT id FROM event_connection WHERE event_id = %s AND location_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, locationId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


def getlocationId(legacyId):
    try:    
        find_sql = """ SELECT id FROM location WHERE (legacy_id = %s OR id = %s) AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, int(legacyId), PROJECT_ID)
    except:
        find_sql = """ SELECT id FROM location WHERE legacy_id = %s AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, PROJECT_ID)
        
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


def getSubjectId(legacyId):
    try:    
        find_sql = """ SELECT id FROM subject WHERE (legacy_id = %s OR id = %s) AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, int(legacyId), PROJECT_ID)
    except:
        find_sql = """ SELECT id FROM subject WHERE legacy_id = %s AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, PROJECT_ID)
    
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


def getPublicationOccurrenceId(eventId, pubId, extId, type='est'):
    # Default
    find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, pubId)
    
    # Override
    if type is 'com':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_comment_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (eventId, pubId, extId)
    elif type is 'ms':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_manuscript_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (eventId, pubId, extId)
    elif type is 'var':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_version_id = %s AND deleted != 1 LIMIT 1 """    
        values_to_find = (eventId, pubId, extId)
    
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


def getSubjectEventConnectionId(eventId, subjectId):
    find_sql = """ SELECT id FROM event_connection WHERE event_id = %s AND subject_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, subjectId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    found_id = None
    if result is not None:
        found_id = result[0]
    return found_id


# Event 
def createEvent(subjectId):
    # Create an event for the publication
    e_insert_sql = """INSERT INTO event (type, description) VALUES('publication', %s) RETURNING id"""
    values_to_find = (subjectId, )
    cursor_new.execute(e_insert_sql, values_to_find)
    eventId = cursor_new.fetchone()[0]
    return eventId


# Event Connection - the Subject is Connected to the Event 
def createLocationEventConnection(locationId, eventId):
    # Check if we already have a connection
    eventConnectionId = getLocationEventConnectionId(eventId, locationId)
    if eventConnectionId is None:
        # Return the connection if we have it, otherwise create it
        ec_insert_sql = """INSERT INTO event_connection (type, location_id, event_id) VALUES('publication-location-occurrence', %s, %s) RETURNING id"""
        values_to_insert = (locationId, eventId)
        cursor_new.execute(ec_insert_sql, values_to_insert)
        eventConnectionId = cursor_new.fetchone()[0]
        return eventConnectionId
    else:
        return eventConnectionId
    
    
# Event Connection - the Subject is Connected to the Event 
def createSubjectEventConnection(subjectId, eventId):
    # Check if we already have a connection
    eventConnectionId = getSubjectEventConnectionId(eventId, subjectId)
    if eventConnectionId is None:
        # Return the connection if we have it, otherwise create it
        ec_insert_sql = """INSERT INTO event_connection (type, subject_id, event_id) VALUES('publication-subject-occurrence', %s, %s) RETURNING id"""
        values_to_insert = (subjectId, eventId)
        cursor_new.execute(ec_insert_sql, values_to_insert)
        eventConnectionId = cursor_new.fetchone()[0]
        return eventConnectionId
    else:
        return eventConnectionId
    

# Get from db
def getCommentId(publication_id):
    find_sql = """ SELECT publication_comment_id FROM publication WHERE id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (publication_id, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    comment_id = None
    if result is not None:
        comment_id = result[0]
    return comment_id


# Get from file path
def getManuscriptId(file_name):
    parts = file_name.split('_')
    return str(parts[len(parts) -1]).replace('.xml','')


# Get from file path
def getVersionId(file_name):
    parts = file_name.split('_')
    return str(parts[len(parts) -1]).replace('.xml','')


# Connect the Publication to an Event
def addPublictionOccurrence(event_id, publication_id, file_name):
    # default to EST
    occId = getPublicationOccurrenceId(event_id, publication_id, None, 'est')
    if occId is None:
        eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id) VALUES('publication-est', %s, %s) RETURNING id"""
        values_to_insert = (event_id, publication_id)
        cursor_new.execute(eo_insert_sql, values_to_insert)
        event_connection_id = cursor_new.fetchone()[0]
    # add the commentary reference if it exists
    if 'com' in file_name:
        comment_id = getCommentId(publication_id)
        occId = getPublicationOccurrenceId(event_id, publication_id, comment_id, 'com')
        if comment_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_comment_id) VALUES('publication-com', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, comment_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_connection_id = cursor_new.fetchone()[0]
    # add the version reference if it exists
    elif 'var' in file_name:
        version_id = getVersionId(file_name)
        occId = getPublicationOccurrenceId(event_id, publication_id, version_id, 'var')
        if version_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_version_id) VALUES('publication-var', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, version_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_connection_id = cursor_new.fetchone()[0]
    # add the manuscript reference if it exists
    elif 'ms' in file_name:
        manuscript_id = getManuscriptId(file_name)
        occId = getPublicationOccurrenceId(event_id, publication_id, manuscript_id, 'ms')
        if manuscript_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_manuscript_id) VALUES('publication-ms', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, manuscript_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_connection_id = cursor_new.fetchone()[0]
    
