from setup import *

def remove_old_subject_connections():
    e_update_sql = """ UPDATE event_connection SET subject_id = NULL, date_modified=now() WHERE subject_id IN ( select id from subject where project_id = %s ) AND work_manifestation_id is NULL AND type='publication-subject-occurrence' """
    values_to_find = (PROJECT_ID, )
    cursor_new.execute(e_update_sql, values_to_find)


def remove_old_location_connections():
    e_update_sql = """ UPDATE event_connection SET location_id = NULL, date_modified=now() WHERE location_id IN ( select id from location where project_id = %s ) AND work_manifestation_id is NULL AND type='publication-location-occurrence' """
    values_to_find = (PROJECT_ID, )
    cursor_new.execute(e_update_sql, values_to_find)
    

def remove_empty_event_connections():
    e_delete_sql = """ UPDATE event_connection SET deleted = 1, date_modified = now() WHERE subject_id is NULL AND tag_id IS NULL AND location_id IS NULL AND work_manifestation_id is NULL AND correspondence_id IS NULL """
    cursor_new.execute(e_delete_sql)


def remove_event_connection(type, event_connection_id):
    if type == "subject":
        e_update_sql = """ UPDATE event_connection SET subject_id = NULL, date_modified=now() WHERE id = %s AND type='publication-subject-occurrence' RETURNING id """
    elif type == "location":
        e_update_sql = """ UPDATE event_connection SET location_id = NULL, date_modified=now() WHERE id = %s AND type='publication-location-occurrence' RETURNING id """
    values_to_find = (event_connection_id, )
    cursor_new.execute(e_update_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_event_id(publicationId):
    find_sql = """ SELECT id FROM event WHERE id in (select event_id from event_occurrence where publication_id = %s and deleted != 1) AND deleted != 1 """
    values_to_find = (publicationId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_event_id_for_subject_connection(publicationId):
    find_sql = """ SELECT id FROM event WHERE id in (select event_id from event_occurrence where publication_id = %s and deleted != 1) AND id in (select event_id from event_connection where subject_id is not null and deleted != 1) AND deleted != 1 """
    values_to_find = (publicationId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_event_id_for_tag_publication_connection(publicationId, tagId):
    find_sql = """ SELECT id FROM event WHERE id in (select event_id from event_occurrence where publication_id = %s and deleted != 1) AND id in (select event_id from event_connection where tag_id = %s and deleted != 1) AND deleted != 1 """
    values_to_find = (publicationId, tagId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_location_event_connection_id(eventId, locationId):
    find_sql = """ SELECT id FROM event_connection WHERE event_id = %s AND location_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, locationId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_location_id(legacyId):
    try:    
        find_sql = """ SELECT id FROM location WHERE (legacy_id = %s OR id = %s) AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, int(legacyId), PROJECT_ID)
    except:
        find_sql = """ SELECT id FROM location WHERE legacy_id = %s AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, PROJECT_ID)
        
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_subject_id(legacyId):
    try:    
        find_sql = """ SELECT id FROM subject WHERE (legacy_id = %s OR id = %s) AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, int(legacyId), PROJECT_ID)
    except:
        find_sql = """ SELECT id FROM subject WHERE legacy_id = %s AND project_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (legacyId, PROJECT_ID)
    
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_publication_occurrence_id(eventId, pubId, extId, type='est'):
    # Default
    find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, pubId)
    
    # Override
    if type == 'com':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_comment_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (eventId, pubId, extId)
    elif type == 'ms':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_manuscript_id = %s AND deleted != 1 LIMIT 1 """
        values_to_find = (eventId, pubId, extId)
    elif type == 'var':
        find_sql = """ SELECT id FROM event_occurrence WHERE event_id = %s AND publication_id = %s AND publication_version_id = %s AND deleted != 1 LIMIT 1 """    
        values_to_find = (eventId, pubId, extId)
    
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_subject_event_connection_id(eventId, subjectId):
    find_sql = """ SELECT id FROM event_connection WHERE event_id = %s AND subject_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, subjectId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_tag_event_connection_id(eventId, tagId):
    find_sql = """ SELECT id FROM event_connection WHERE event_id = %s AND tag_id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (eventId, tagId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_all_subject_event_connections_for_publication(pubId):
    find_sql = """ SELECT id, subject_id FROM event_connection WHERE event_id IN (select event_id from event_occurrence where publication_id = %s and deleted != 1) AND subject_id IS NOT NULL AND deleted != 1 """
    values_to_find = (pubId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchall()
    return result


def get_all_tag_event_conns_for_pub(pubId):
    find_sql = """ SELECT event_id, tag_id FROM event_connection WHERE event_id IN (select event_id from event_occurrence where publication_id = %s and deleted != 1) AND tag_id IS NOT NULL AND deleted != 1 """
    values_to_find = (pubId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchall()
    return result


# Sets deleted = 1 for event with id eventId in table event
def delete_event(eventId):
    e_update_sql = """ UPDATE event SET deleted = 1, date_modified=now() WHERE id = %s AND deleted != 1 """
    values_to_find = (eventId, )
    cursor_new.execute(e_update_sql, values_to_find)


def delete_event_connection(eventId, targetType, targetId):
    if targetType == 'subject':
        ec_update_sql = """UPDATE event_connection SET subject_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND subject_id = %s AND deleted != 1 """
    elif targetType == 'tag':
        ec_update_sql = """UPDATE event_connection SET tag_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND tag_id = %s AND deleted != 1 """
    elif targetType == 'location':
        ec_update_sql = """UPDATE event_connection SET location_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND location_id = %s AND deleted != 1 """
    elif targetType == 'work_manifestation':
        ec_update_sql = """UPDATE event_connection SET work_manifestation_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND work_manifestation_id = %s AND deleted != 1 """
    elif targetType == 'correspondence':
        ec_update_sql = """UPDATE event_connection SET correspondence_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND correspondence_id = %s AND deleted != 1 """
    else:
        return
    values_to_find = (eventId, targetId)
    cursor_new.execute(ec_update_sql, values_to_find)


def delete_event_occurrence(eventId, publicationId):
    eo_update_sql = """UPDATE event_occurrence SET publication_id = NULL, publication_version_id = NULL, publication_manuscript_id = NULL, publication_facsimile_id = NULL, publication_comment_id = NULL, publication_facsimile_page = NULL, publication_song_id = NULL, work_id = NULL, deleted = 1, date_modified=now() WHERE event_id = %s AND publication_id = %s AND deleted != 1 """
    values_to_find = (eventId, publicationId)
    cursor_new.execute(eo_update_sql, values_to_find)


# Insert new event into database and return its id
def create_event(typeText, descriptionText):
    # Create an event for the publication
    e_insert_sql = """INSERT INTO event (type, description) VALUES(%s, %s) RETURNING id"""
    values_to_find = (typeText, descriptionText)
    cursor_new.execute(e_insert_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


# Event Connection - the Subject is Connected to the Event 
def create_location_event_connection(locationId, eventId):
    # Check if we already have a connection
    eventConnectionId = get_location_event_connection_id(eventId, locationId)
    if eventConnectionId is None:
        # Return the connection if we have it, otherwise create it
        ec_insert_sql = """INSERT INTO event_connection (type, location_id, event_id) VALUES('publication-location-occurrence', %s, %s) RETURNING id"""
        values_to_insert = (locationId, eventId)
        cursor_new.execute(ec_insert_sql, values_to_insert)
        eventConnectionId = cursor_new.fetchone()[0]
        if eventConnectionId is not None and eventConnectionId > 0:
            return 1
        else:
            return 0
    else:
        return 0
    
    
# Event Connection - the Subject is Connected to the Event 
def create_subject_event_connection(subjectId, eventId):
    # Check if we already have a connection
    eventConnectionId = get_subject_event_connection_id(eventId, subjectId)
    if eventConnectionId is None:
        # Return the connection if we have it, otherwise create it
        ec_insert_sql = """INSERT INTO event_connection (type, subject_id, event_id) VALUES('publication-subject-occurrence', %s, %s) RETURNING id"""
        values_to_insert = (subjectId, eventId)
        cursor_new.execute(ec_insert_sql, values_to_insert)
        eventConnectionId = cursor_new.fetchone()[0]
        if eventConnectionId is not None and eventConnectionId > 0:
            return 1
        else:
            return 0
    else:
        return 0


def create_event_connection(eventId, targetType, targetId, type = None):
    if targetType == 'subject':
        ec_insert_sql = """INSERT INTO event_connection (event_id, subject_id, type) VALUES(%s, %s, %s) RETURNING id"""
    elif targetType == 'tag':
        ec_insert_sql = """INSERT INTO event_connection (event_id, tag_id, type) VALUES(%s, %s, %s) RETURNING id"""
    elif targetType == 'location':
        ec_insert_sql = """INSERT INTO event_connection (event_id, location_id, type) VALUES(%s, %s, %s) RETURNING id"""
    elif targetType == 'work_manifestation':
        ec_insert_sql = """INSERT INTO event_connection (event_id, work_manifestation_id, type) VALUES(%s, %s, %s) RETURNING id"""
    elif targetType == 'correspondence':
        ec_insert_sql = """INSERT INTO event_connection (event_id, correspondence_id, type) VALUES(%s, %s, %s) RETURNING id"""
    else:
        return None
    values_to_insert = (eventId, targetId, type)
    cursor_new.execute(ec_insert_sql, values_to_insert)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def create_event_occurrence(eventId, publicationId, type = None, description = None, extType = None, extId = None, facsPage = None):
    if extType == 'var':
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id, publication_version_id) VALUES(%s, %s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId, extId)
    elif extType == 'ms':
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id, publication_manuscript_id) VALUES(%s, %s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId, extId)
    elif extType == 'com':
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id, publication_comment_id) VALUES(%s, %s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId, extId)
    elif extType == 'work':
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id, work_id) VALUES(%s, %s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId, extId)
    elif extType == 'facs':
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id, publication_facsimile_id, publication_facsimile_page) VALUES(%s, %s, %s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId, extId, facsPage)
    else:
        eo_insert_sql = """INSERT INTO event_occurrence (type, description, event_id, publication_id) VALUES(%s, %s, %s, %s) RETURNING id"""
        values_to_insert = (type, description, eventId, publicationId)
    cursor_new.execute(eo_insert_sql, values_to_insert)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None



# Can be used to check if publication exists
def get_publication_id(publicationId):
    find_sql = """ SELECT id FROM publication WHERE id = %s AND deleted != 1 """
    values_to_find = (publicationId, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


# Get from db
def get_comment_id(publication_id):
    find_sql = """ SELECT publication_comment_id FROM publication WHERE id = %s AND deleted != 1 LIMIT 1 """
    values_to_find = (publication_id, )
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchone()
    comment_id = None
    if result is not None:
        comment_id = result[0]
    return comment_id


# Get from file path
def get_manuscript_id(file_name):
    parts = file_name.split('_')
    return str(parts[len(parts) -1]).replace('.xml','')


# Get from file path
def get_version_id(file_name):
    parts = file_name.split('_')
    return str(parts[len(parts) -1]).replace('.xml','')


# Connect the Publication to an Event
def add_publication_occurrence(event_id, publication_id, file_name):
    # default to EST
    occId = get_publication_occurrence_id(event_id, publication_id, None, 'est')
    if occId is None:
        eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id) VALUES('publication-est', %s, %s) RETURNING id"""
        values_to_insert = (event_id, publication_id)
        cursor_new.execute(eo_insert_sql, values_to_insert)
        event_occurrence_id = cursor_new.fetchone()[0]
    # add the commentary reference if it exists
    if 'com' in file_name:
        comment_id = get_comment_id(publication_id)
        occId = get_publication_occurrence_id(event_id, publication_id, comment_id, 'com')
        if comment_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_comment_id) VALUES('publication-com', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, comment_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_occurrence_id = cursor_new.fetchone()[0]
    # add the version reference if it exists
    elif 'var' in file_name:
        version_id = get_version_id(file_name)
        occId = get_publication_occurrence_id(event_id, publication_id, version_id, 'var')
        if version_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_version_id) VALUES('publication-var', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, version_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_occurrence_id = cursor_new.fetchone()[0]
    # add the manuscript reference if it exists
    elif 'ms' in file_name:
        manuscript_id = get_manuscript_id(file_name)
        occId = get_publication_occurrence_id(event_id, publication_id, manuscript_id, 'ms')
        if manuscript_id is not None and occId is not None:
            eo_insert_sql = """INSERT INTO event_occurrence (type, event_id, publication_id, publication_manuscript_id) VALUES('publication-ms', %s, %s, %s) RETURNING id"""
            values_to_insert = (event_id, publication_id, manuscript_id)
            cursor_new.execute(eo_insert_sql, values_to_insert)
            event_occurrence_id = cursor_new.fetchone()[0]
    else:
        return event_occurrence_id
    

def get_tag(legacyId, projectId, usingLegacyId = True):
    if usingLegacyId:
        find_sql = """ SELECT * FROM tag WHERE legacy_id = %s AND project_id = %s AND deleted != 1 """
    else:
        find_sql = """ SELECT * FROM tag WHERE id = %s AND project_id = %s AND deleted != 1 """
    values_to_find = (legacyId, projectId)
    cursor_new.execute(find_sql, values_to_find)
    result = cursor_new.fetchall()
    return result


def create_tag(legacyId, name, type, description, source, projectId):
    t_insert_sql = """INSERT INTO tag (legacy_id, name, type, description, source, project_id) VALUES(%s, %s, %s, %s, %s, %s) RETURNING id"""
    values_to_find = (legacyId, name, type, description, source, projectId)
    cursor_new.execute(t_insert_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def update_tag(legacyId, name, type, description, source, projectId):
    t_update_sql = """ UPDATE tag SET name = %s, type = %s, description = %s, source = %s, date_modified=now() WHERE legacy_id = %s AND project_id = %s RETURNING id """
    values_to_find = (name, type, description, source, legacyId, projectId)
    cursor_new.execute(t_update_sql, values_to_find)
    result = cursor_new.fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def get_user_confirmation_for_db_update():
    response = ""
    while (response != "yes" and response != "no"):
        response = input("Please confirm that the database should be updated (answer yes or no): ")
        response = response.lower()
        if response == "yes":
            return True
        elif response == "no":
            return False
        else:
            print("Invalid input. Answer 'yes' to update database, or 'no' not to.")
