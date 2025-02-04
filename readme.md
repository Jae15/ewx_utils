# ewx_utils Project

## The process of running this ewx_utils project in another machine

- The first step needed to run this project is to create an SSH key through a tool such as PuTTYgen
- This project requires a key to set up sftp.geo.msu.edu SSH tunnel
- Use the sftp tunnel to connect to the databases of choice eg supercell and dbh11
- To set up/configure the ports, databases and the SSH tunnel, one could use a tool such as PuTTy

## Setup

### How to install packages for the project
- This project is tested and known to work with Python version 3.12

- The project requires external python packages, listed in located in the requirements.txt file
   To install them, run 

```
pip install -r requirements.txt
```

### Configuration

Configuration values for the scripts are in the file `.env` which uses the 
format defined by the package `python-dotenv`.  For details about this file, 
see https://pypi.org/project/python-dotenv/#getting-started.    An example `.env`
file is in the main direction, called `example-dot-env.txt`   Copy this file to 
a new file name `.env` and edit the values.   

The `.env` file contains three variables: 

- `EWX_BASE_PATH`=the absolute path to the top folder of this project. 
- `DATABASE_CONFIG_FILE`=the absolute path to the file containing the database 
   connection information (database name, use, and passwords)
- `EWX_LOG_FILE`=the absolute path to a folder that will contain log files. the
   folder must exist on your system

There are no restrictions on these files but they must be defined for the 
scripts to run properly and this program must read access to all of them, 
and write-access to the file path in `EWX_LOG_FILE`

Note that these environment variables can be set prior to running the python
scripts and the `load_dotenv()` function will use the currently set values. 

### Database configuration

- It requires a postgres database and uses pyscopg2 to connect python with the database
- It requires a database connection configuration file, using the file path 
  indicated by the configuration value `DATABASE_CONFIG_FILE` in .env
  There is no default value so you must set this, but typically set to `database.ini` 
- The database connection configuration file follows the syntax outline in the 
  [ConfigParser](https://docs.python.org/3/library/configparser.html) python
  library, or "ini" format (and usually has an 'ini' extension). 
- This file is to have a section as defined in each function in the file
  `ewx_utils/db_files/dbs_configfile.py` which is based on which databases are 
  currently available from Enviroweather servers.  When the names and servers of
  the main Enviroweather databases change, you must edit both the file 
  `ewx_utils/db_files/dbs_configfile.py` and the database ini file in
   `DATABASE_CONFIG_FILE` (often `database.ini` but not necessarily)
- An example section would look like:


```
[mawnqc_dbh11]
host = 12.34.56.78
port = 5432
dbname = mawndb_qc
user = mydbusername
password = mydbpassword
```

> When running on your own computer, you may use ssh-tunnel to connect to the 
> Enviroweather database server, and the host may be `127.0.0.1` and port the one
> used by the tunnel.   See the Enviroweather system administrator for connection
> details

- Two of the database entries are required for testing.  You can run either create
  these on an remote server, or run a Postgresql server 
  on your local computer.  Create an empty databases using the schema file for the 
  `mawn_qc` database. 

## How to run the hourly_main.py script (project entry script)

The entry script to the project is the hourly_main.py located in the main_hourly_scripts folder.

The following commands are be used to run the hourly_main.py entry file that fetches data from one databases and inserts/updates into another.


```
cd ewx_utils/main_hourly_scripts

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





