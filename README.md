# digital_edition_events
Scripts for creating events, event occurrences and event connections for subjects and locations

## How to use

1. Write database connection parameters into the .env file (rename .env_example).
2. Copy the published xml-files (not the originals) to the xml-folder (est, com, ms and/or var-files).
3. Run subject_xml_occurrences.py to create events for subjects, and location_xml_occurrences.py to create events for locations.

The scripts take three command line arguments, of which only the first is obligatory:<br>PROJECT_ID DEBUG REMOVE_OLD.

Set DEBUG to 1 to run the script in debug mode, which means no data is committed to the database. Set it to 0 to run in normal mode, i.e. data is committed to the database.

Set REMOVE_OLD to 1 to have the script first remove all old connections in the database for the entire project. Set it to 0 to not remove old connections.

If DEBUG and REMOVE_OLD are omitted, DEBUG is set to true and REMOVE_OLD to false.
