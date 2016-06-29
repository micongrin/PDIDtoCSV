# PDIDtoCSV
Python script to export PDID (MDID) collections direct to csv

## Bits to note
- Duplicate the mysql database on your local machine
- The script will connect to the local database
- I did the migration in batches, so as it stands, the script takes one argument (start date) and pulls images going back three months from the start date provided.
- In addition to writing a csv from data in the *fielddata* table this script copies the actual image files, which you may wish to remove
