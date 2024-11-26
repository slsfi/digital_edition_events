######################## ABOUT THIS SCRIPT ########################
#
# Version: 1.0
#
# This script is used to insert new tags in the tag-table of the
# database, or to update existing tags. The tags are read from
# a csv-file with the following fields:
# - legacy_id
# - name
# - type
# - description
# - source
# legacy_id is the only required field. The order of the fields
# in the csv-file is irrelevant. The file must contain headers
# with the proper field names. The csv-file must be UTF-8
# encoded.
#
# To delete a tag, you should first remove it's connections to
# publications using add_tag_occurrences_from_csv.py, and then
# manually mark the tag deleted in the database.
#
# Setup: Fill in the parameters in the .env file.
#
# Run with command line argument:
# DEBUG
#
# DEBUG: 1 to run in debug mode which doesn't commit data to the
#   database. 0 to run in normal mode.
#
# Created by Sebastian Köhler 2022-10-27.
#
# Change log:
#   - 2022-10-27: v1.0
#
###################################################################
import os
import logging
import csv

from setup import *
from general import *


logging.basicConfig(filename="logs/add_tags_from_csv_log.txt",
                level=logging.DEBUG,
                format='%(levelname)s: %(asctime)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S')


def add_tags_from_csv_file():
    try:
        # Get row count in csv-file for progress output
        rowCount = 0
        with open(INPUT_FILEPATH, encoding='utf-8') as f:
            rowCount = sum(1 for line in f)

        # Open csv-file and initiate csv-reader
        with open(INPUT_FILEPATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile,
                                    restkey="extra_fields",
                                    delimiter=CSV_DELIMITER,
                                    quotechar=CSV_QUOTECHAR,
                                    doublequote=CSV_DOUBLEQUOTE,
                                    escapechar=CSV_ESCAPECHAR,
                                    quoting=CSV_QUOTING)
            update_database_with_tags(reader, rowCount - 1)
    except FileNotFoundError as error:
        print(f"Unable to open the csv-file {INPUT_FILEPATH}. Terminating script.")
        print(error)
        exit()


def update_database_with_tags(tagsReader, totalRows):
    global DEBUG
    insertedCount = 0
    updatedCount = 0
    validRowsCount = 0
    rowCounter = 0
    progress = 1
    progressDivisor = 20

    if DEBUG is False:
        confirmed = get_user_confirmation_for_db_update()
        if confirmed is False:
            DEBUG = True
            print("Running script in debug mode. No data will be committed to the database.", end="\n\n")

    print(f"Processing {totalRows} rows in the csv-file.", end="\n\n")
    print("Progress:")
    print("0         50        100 %")
    print("|", end="", flush=True)

    for row in tagsReader:
        if "legacy_id" not in row.keys():
            print("The csv-file is missing legacy_id in header fields. Terminating script.")
            exit()

        # Print progress to screen
        if totalRows > progressDivisor:
            if rowCounter > (((totalRows-1) / progressDivisor) * progress):
                print("-", end="", flush=True)
                progress += 1

        # Validate and format field values
        if row['legacy_id'] is None or row['legacy_id'] == "":
            logging.warning("Csv-file has row with empty legacy_id, skipping row.")
            continue
        else:
            validRowsCount += 1

        legacyId = TAG_LEGACY_ID_PREFIX + row['legacy_id']
        name = None
        type = None
        description = None
        source = None

        if "name" in row.keys():
            if row['name'] == "":
                name = None
            else:
                name = row['name']

        if name is None:
            logging.warning("Tag with legacy_id '{}' has empty name field.".format(legacyId))

        if "type" in row.keys():
            if row['type'] == "":
                type = None
            else:
                type = row['type']

        if "description" in row.keys():
            if row['description'] == "":
                description = None
            else:
                description = row['description']

        if "source" in row.keys():
            if row['source'] == "":
                source = None
            else:
                source = row['source']

        # Check if tag already exists in database using legacy_id
        tagId = None
        tagDataset = get_tag(legacyId, PROJECT_ID, True, True)

        # Filter non-deleted and deleted tags from all returned tags
        non_deleted_tags = [row for row in tagDataset if row['deleted'] < 1]
        deleted_tags = [row for row in tagDataset if row['deleted'] > 0]

        if len(non_deleted_tags) > 1:
            logging.warning("Multiple non-deleted tags with legacy_id '{}' exist in the database. The data of this tag has not been updated in the database.".format(legacyId))
        elif len(non_deleted_tags) < 1 and len(deleted_tags) < 1:
            # Create tag
            tagId = create_tag(legacyId, name, type, description, source, PROJECT_ID)
            insertedCount += 1
            if DEBUG is False:
                logging.info("Inserted tag with id '{}' and legacy_id '{}' into database".format(tagId, legacyId))
            else:
                logging.info("Debug mode: tag with id '{}' and legacy_id '{}' could be inserted into database".format(tagId, legacyId))
        elif len(deleted_tags) > 0 and len(non_deleted_tags) < 1:
            logging.info("Skipping tag with legacy_id '{}'; it is marked as deleted in the database.".format(legacyId))
        elif len(non_deleted_tags) > 0:
            # Tag exists, update using legacy_id if data has changed
            tagData = non_deleted_tags[0]
            dataUnchanged = compare_tag_data(tagData, name, type, description, source)

            if dataUnchanged == False:
                tagId = update_tag(legacyId, name, type, description, source, PROJECT_ID)
                updatedCount += 1
                if DEBUG is False:
                    logging.info("Updated tag with id '{}' and legacy_id '{}' in database".format(tagId, legacyId))
                else:
                    logging.info("Debug mode: tag with id '{}' and legacy_id '{}' could be updated in database".format(tagId, legacyId))

        if DEBUG is False and tagId is not None:
            conn_new_db.commit()
        rowCounter += 1

    if totalRows > progressDivisor:
        print("-|")
    else:
        print("--------------------|")
    print()

    if DEBUG is False:
        print(f"Successfully processed {validRowsCount} rows from csv-file.")
        print(f"Inserted {insertedCount} new tags in database.")
        print(f"Updated {updatedCount} existing tags in database.")
        print(f"{validRowsCount - updatedCount - insertedCount} existing tags in database were already up to date.", end="\n\n")
    else:
        print(f"Successfully processed {validRowsCount} rows from csv-file in debug mode.\nNo changes have been made to the database.", end="\n\n")
        print(f"{insertedCount} new tags could be inserted in the database.")
        print(f"{updatedCount} existing tags could be updated in the database.")
        print(f"{validRowsCount - updatedCount - insertedCount} existing tags in database were already up to date.", end="\n\n")
    logging.info("-----------------------------------------------------")
        

def compare_tag_data(dbTagData, csvName, csvType, csvDescription, csvSource):
    if (dbTagData['name'] != csvName
            or dbTagData['type'] != csvType 
            or dbTagData['description'] != csvDescription
            or dbTagData['source'] != csvSource):
        return False
    else:
        return True


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

    add_tags_from_csv_file()

    if DEBUG is False:
        conn_new_db.commit()
    conn_new_db.close()


if __name__ == "__main__":
    main()
