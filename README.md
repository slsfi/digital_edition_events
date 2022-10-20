# digital_edition_events
Scripts for creating events, event occurrences and event connections for subjects and locations

## How to use

1. Write database connection parameters into the .env file.
2. Copy the published xml-files (not the originals) to the xml-folder (est, com, ms and/or var-files).
3. Run subject_xml_occurrences.py to create events for subjects, and location_xml_occurrences.py to create events for locations. The scripts take three arguments, of which only the first is obligatory: PROJECT_ID DEBUG REMOVE_OLD. Set DEBUG to 1 to run the script in debug mode, anything else is treated as a false value. Set REMOVE_OLD to 1 to have the script first remove all old connections for the given xml-files. Anything else is treated as a false value. If DEBUG and REMOVE_OLD are omitted, DEBUG is set to true and REMOVE_OLD to false.