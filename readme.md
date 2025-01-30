# ewx_utils Project

# The process of running this ewx_utils project in another machine
- The first step needed to run this project is to create an SSH key through a tool such as PuTTYgen
- This project requires a key to set up sftp.geo.msu.edu SSH tunnel
- Use the sftp tunnel to connect to the databases of choice eg supercell and dbh11
- To set up/configure the ports, databases and the SSH tunnel, one could use a tool such as PuTTy

## How to install packages for the project
- The project packages are located in the requirements.txt file
- This project works with python 3.12
- It requires a postgres database and uses pyscopg2 to connect python with the database

```
pip install -r requirements.txt
```

## How to run the hourly_main.py script (project entry script)
- The entry script to the project is the hourly_main.py located in the main_hourly_scripts folder.
- The following commands are be used to run the hourly_main.py entry file that fetches data from one databases and inserts/updates into another.
```
python hourly_main.py -a -x
python hourly_main.py --begin 2024-02-03 --end 2024-02-08 -a -x
python hourly_main.py --begin 2023-02-01 --end 2023-02-02 --station aetna -x

hourly_main [-h] [-b BEGIN] [-e END] [-f] [-c] (-x | -d) [-l] [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}] [--rtma {rtma:dbh11}]
```
## How to run the hourly_utility.py script
- The main utility script for the ewx_utils project is the hourly_utility.py script.
- This script is used to compare records in the source database to the records in the destination database.
- A test database is used to mimick the destination database - records from the source database are stored here and compared to the destination database.
- Mismatches observed are stored in csv files for easier comparisons
- This script helps to ensure data accuracy and consistency across the board and an easy way to narrow down to problematic values in our database as well as identifying bugs in the project code.
- Below is the command used to run the main utility script as well as the required arguments.

```
python hourly_utility.py --begin 2023-01-01 --end 2023-01-02 --station aetna_hourly

usage: hourly_utility.py [-h] -b BEGIN -e END -s STATION
required: -b/--begin, -e/--end, -s/--station
```

## How to run the clear_records.py script
- The clear_records.py script is also a utility script created to be used for dryrun purposes only.
- During the code testing phase, this script is useful in clearing/deleting faulty records in order to later reinsert clean records into a clean database.
- This script is purely for testing and dryrun purposes.
- Below is the command used to run the clear_records.py script as well as the required arguments.

```
python clear_records.py -x -s arlene -q mawnqc_test:local
usage: clear_records [-h] (-x | -d) [-s [STATIONS ...] | -a] [-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}]
required arguments: -x/--execute -d/--dryrun
# To clear records from all the stations
python clear_records.py -x -a -q mawnqc_test:local
# To clear records from specific stations
python clear_records.py -x -s station1 station2 -q mawnqc_test:local
# For a dry-run
python clear_records.py -d -a -q mawnqc_test:local
```





