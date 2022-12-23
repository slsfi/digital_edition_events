######################## ABOUT THIS SCRIPT ########################
#
# Version: 1.0
#
# The input csv-file must not contains a header row. The first
# field on each row must be publication id, the second field is
# skipped and can contain anything, e.g. publication name. The
# following fields must contain tag information in a format
# where the legacy_id without prefix is inside brackets, e.g.
# mjölk [10].
#
# Setup: Fill in the parameters in the .env file.
#
# Run with command line arguments:
# DEBUG
#
# DEBUG: 1 to run in debug mode which doesn't commit data to the
#   database. 0 to run in normal mode.
#
# Created by Sebastian Köhler 2022-10-28.
#
# Change log:
#   - 2022-10-28: v1.0
#
###################################################################
import os
import re
import logging
import csv

from setup import *
from general import *


logging.basicConfig(filename="logs/add_tag_occurrences_from_csv_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')


def add_tag_occurrences_from_csv_file():
    try:
        # Get row count in csv-file for progress output
        row_count = 0
        with open(INPUT_FILEPATH, encoding='utf-8') as f:
            row_count = sum(1 for line in f)

        # Open csv-file and initiate csv-reader
        with open(INPUT_FILEPATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile,
                                    delimiter=CSV_DELIMITER,
                                    quotechar=CSV_QUOTECHAR,
                                    doublequote=CSV_DOUBLEQUOTE,
                                    escapechar=CSV_ESCAPECHAR,
                                    quoting=CSV_QUOTING)
            update_database_with_tag_occurrences(reader, row_count - 1)
    except FileNotFoundError as error:
        print(f"Unable to open the csv-file {INPUT_FILEPATH}. Terminating script.")
        print(error)
        exit()
    except UnicodeDecodeError as decode_error:
        print("The input file is not in utf-8 encoding. Please convert to utf-8 and run script again.")
        print(decode_error)
        exit()


def update_database_with_tag_occurrences(tags_reader, total_rows):
    global DEBUG
    inserted_count = 0
    unchanged_count = 0
    duplicate_count = 0
    removed_count = 0
    error_count = 0
    valid_rows_count = 0
    row_counter = 0
    progress = 1
    progress_divisor = 20

    if DEBUG is False:
        confirmed = get_user_confirmation_for_db_update()
        if confirmed is False:
            DEBUG = True
            print("Running script in debug mode. No data will be committed to the database.", end="\n\n")

    print(f"Processing {total_rows} rows in the csv-file.", end="\n\n")
    print("Progress:")
    print("0         50        100 %")
    print("|", end="", flush=True)

    # Loop through each row in the csv-file, i.e. process each publication with connected tags
    for row in tags_reader:
        # Print progress to screen
        if total_rows > progress_divisor:
            if row_counter > (((total_rows-1) / progress_divisor) * progress):
                print("-", end="", flush=True)
                progress += 1
        
        # Skip the row if the first field doesn't consist of just digits,
        # i.e. it's not a publication id. Assume that the first row
        # consists of headers.
        if str(row[0]).isdigit() == False:
            if row_counter > 0:
                logging.warning(f"The first field on row {row_counter + 1} in the csv-file is not a publication id. Skipping row.")
                error_count += 1
            row_counter += 1
            continue

        publication_id = int(row[0])

        logging.info(f"Processing publication with id {publication_id}.")

        # Check that the publication exists, if it doesn't, skip row
        if get_publication_id(publication_id) is None:
            logging.warning(f"------- The publication is missing from the database. Skipping row.")
            error_count += 1
            row_counter += 1
            continue

        publication_tags_in_file = []

        # Loop through all fields containing tag legacy ids and insert into database if not already connected
        for index in range(len(row)):
            # Skip the first two fields in the row since they contain publication id and title
            if index > 1 and row[index] != "":
                # Extract the tag legacy id from the field (assume it's
                # inside brackets)
                m = re.search(r"\[([0-9]+?)\]", row[index])
                if m and m.group(1) is not None and m.group(1).isdigit():
                    tag_legacy_id = TAG_LEGACY_ID_PREFIX + m.group(1)
                    # print(f"Index {index} id {tag_legacy_id}")
                    tag_data = get_tag(tag_legacy_id, PROJECT_ID)
                    if len(tag_data) > 1:
                        logging.warning(f"------- Multiple non-deleted tags with legacy_id '{tag_legacy_id}' exist in the database. Tag not connected to publication.")
                        error_count += 1
                    elif len(tag_data) < 1:
                        logging.warning(f"------- Missing tag in database, legacy_id '{tag_legacy_id}'. Tag not connected to publication.")
                        error_count += 1
                    else:
                        # Tag exists, connect tag and publication through an event.
                        # Check if the tag and publication are already connected.
                        # A tag can be connected to a publication established
                        # text only once, i.e. they have only one occurrence.
                        tag_id = tag_data[0]['id']
                        if tag_id not in publication_tags_in_file:
                            publication_tags_in_file.append(tag_id)
                            event_id = get_event_id_for_tag_publication_connection(publication_id, tag_id)
                            if event_id is None:
                                # Create event, connect tag to it, and create a
                                # publication occurrence
                                event_id = create_event("publication-tag", "project " + PROJECT_ID)
                                if event_id is None:
                                    logging.warning(f"------- Failed to insert event for tag_id {tag_id} (legacy_id {tag_legacy_id}). Event connection and occurrence will not be created.")
                                    error_count += 1
                                else:
                                    event_conn_id = create_event_connection(event_id, "tag", tag_id, "publication-tag-occurrence")
                                    if event_conn_id is None:
                                        logging.warning(f"------- Failed to insert event_connection for event_id {event_id} and tag_id {tag_id} (legacy_id {tag_legacy_id}). Event occurrence will not be created. Attempting to delete the event.")
                                        delete_event(event_id)
                                        error_count += 1
                                    else:
                                        event_occ_id = create_event_occurrence(event_id, publication_id, "tag-occurrence", "project " + PROJECT_ID)
                                        if event_occ_id is None:
                                            logging.warning(f"------- Failed to insert event_occurrence for event_id {event_id} and tag_id {tag_id} (legacy_id {tag_legacy_id}). Attempting to delete the event and event connection.")
                                            delete_event_connection(event_id, "tag", tag_id)
                                            delete_event(event_id)
                                            error_count += 1
                                        else:
                                            inserted_count += 1
                                            if DEBUG is False:
                                                logging.info(f"------- Connected tag_id {tag_id} (legacy_id {tag_legacy_id}) to the publication. event_id is {event_id}.")
                                            else:
                                                logging.info(f"------- Debug mode: tag_id {tag_id} (legacy_id {tag_legacy_id}) could be connected to the publication. event_id is {event_id}.")
                            else:
                                # The tag is already connected to the publication.
                                logging.info(f"------- Tag with id {tag_id} (legacy_id {tag_legacy_id}) already connected to the publication. event_id is {event_id}.")
                                unchanged_count += 1
                        else:
                            # Duplicate tag in input file, skip.
                            logging.info(f"------- Duplicate tag with id {tag_id} (legacy_id {tag_legacy_id}) connected to the publication.")
                            duplicate_count += 1
        
        # All tags have been processed.
        # Next check if there are tags connected to the publication in
        # the database that are missing from the file. These should be
        # removed so that the database reflects the file.
        tag_event_conns = get_all_tag_event_conns_for_pub(publication_id)
        for c in range(len(tag_event_conns)):
            event_id = tag_event_conns[c]['event_id']
            tag_id = tag_event_conns[c]['tag_id']
            if tag_id not in publication_tags_in_file:
                tag_data = get_tag(tag_id, PROJECT_ID, False)
                tag_legacy_id = ""
                if len(tag_data) > 0:
                    tag_legacy_id = tag_data[0]['legacy_id']
                delete_event_connection(event_id, "tag", tag_id)
                delete_event_occurrence(event_id, publication_id)
                delete_event(event_id)
                removed_count += 1
                logging.info(f"------- Removed tag_id {tag_id} (legacy_id {tag_legacy_id}) from the publication.")
        
        if DEBUG is False:
            conn_new_db.commit()
        row_counter += 1
        valid_rows_count += 1

    if total_rows > progress_divisor:
        print("-|")
    else:
        print("--------------------|")
    print()

    if DEBUG is False:
        print(f"Successfully processed {valid_rows_count} rows from csv-file.")
        print(f"Inserted {inserted_count} new connections between tags and publications in database.")
        print(f"{unchanged_count} connections already existed in the database.")
        print(f"{duplicate_count} duplicate connections in input file were skipped.")
        print(f"Removed {removed_count} connections from the database.")
    else:
        print(f"Successfully processed {valid_rows_count} rows from csv-file in debug mode.\nNo changes have been made to the database.", end="\n\n")
        print(f"{inserted_count} new connections between tags and publications could be inserted in the database.")
        print(f"{unchanged_count} connections already existed in the database.")
        print(f"{duplicate_count} duplicate connections in input file were skipped.")
        print(f"{removed_count} connections could be removed from the database.")
    if error_count > 0:
        print(f"There were {error_count} errors, check the log.")
    print()
    logging.info("-----------------------------------------------------")


def main():
    require_project_id_in_env_file()
    require_csv_parameters_in_env_file()
    require_input_filepath_in_env_file()
    require_tag_legacy_id_prefix_in_env_file()
    
    print("\nRunning script with the following parameters:", end="\n\n")
    print("Database host: {}".format(os.getenv("DB_HOST")))
    print(f"Project id: {PROJECT_ID}")
    print(f"Csv filepath: {INPUT_FILEPATH}")
    print(f"Tag-prefix: {TAG_LEGACY_ID_PREFIX}")
    print(f"Debug: {DEBUG}")
    print(f"Csv delimiter: {CSV_DELIMITER}")
    print(f"Csv quotechar: {CSV_QUOTECHAR}")
    print(f"Csv doublequote: {CSV_DOUBLEQUOTE}")
    print(f"Csv escapechar: {CSV_ESCAPECHAR}")
    print("Csv quoting: {}".format(os.getenv("CSV_QUOTING")))
    print()

    add_tag_occurrences_from_csv_file()

    if DEBUG is False:
        conn_new_db.commit()
    conn_new_db.close()


if __name__ == "__main__":
    main()
