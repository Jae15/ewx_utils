# ewx_utils Project

## The process of running this ewx_utils project on another machine

- The first step needed to run this project is to create an SSH key through a tool such as PuTTYgen.
- This project requires a key to set up the `sftp.geo.msu.edu` SSH tunnel.
- Use the SFTP tunnel to connect to the databases of choice, e.g., supercell and dbh11.
- To set up/configure the ports, databases, and the SSH tunnel, one could use a tool such as PuTTY.

## Setup

### How to install packages for the project

- This project is tested and known to work with Python version 3.12.
- The project requires external Python packages, listed in the `requirements.txt` file. To install them, run:

```
pip install -r requirements.txt
```

### Installation

```
pip install python-dotenv
```
1. Clone the repository:
```
git clone <repository-url>
cd ewx_utils
```
2. Run the setup script `env_setup.py`:
### Setup Options

The setup script supports several installation methods:

1. **Default Installation**
python db_files/setup_env.py
- Automatically detects and suggests a path
- Creates necessary directory structure
- Generates configuration files

2. **Custom Path Installation**
python db_file/setup_env.py --path /custom/path
- Installs in specified custom location
- Maintains standard directory structure

3. **Force Installation**
python db_file/setup_env.py --force
- Skips confirmation prompts

### Configuration
On running the env_setup.py file, a `.env` and `.env.example` files are automatically created. These files will contain the automatically generated paths and will be located at the base of the project. 

Configuration values for the scripts are in the file `.env`, which uses the format defined by the package `python-dotenv`. For details about this file, see python-dotenv. An example `.env` file is in the main directory, called `.env.example`. 

### Environment Variables

The setup configures the following environment variables:

- `EWX_BASE_PATH`: Base directory path - The absolute path to the top folder of this project.
- `DATABASE_CONFIG_FILE`: Database configuration file location - The absolute path to the file containing the database connection information (database name, user, and passwords).
- `EWX_LOG_FILE`: Log file directory - the absolute path to a folder that will contain log files. The folder must exist on your system..

There are no restrictions on these files, but they must be defined for the scripts to run properly, and this program must have read access to all of them and write access to the file path in `EWX_LOG_FILE`.

Note that these environment variables can be set prior to running the Python scripts, and the `load_dotenv()` function will use the currently set values.

### Database Configuration

- It requires a PostgreSQL database and uses `psycopg2` to connect Python with the database.
- It requires a database connection configuration file, using the file path indicated by the configuration value `DATABASE_CONFIG_FILE` in `.env`. There is no default value, so you must set this, but it is typically set to `database.ini`.
- The database connection configuration file follows the syntax outlined in the ConfigParser Python library, or "ini" format (and usually has an 'ini' extension).
- This file is to have a section as defined in each function in the file `ewx_utils/db_files/dbs_configfile.py`, which is based on which databases are currently available from Enviroweather servers. When the names and servers of the main Enviroweather databases change, you must edit both the file `ewx_utils/db_files/dbs_configfile.py` and the database ini file in `DATABASE_CONFIG_FILE` (often `database.ini` but not necessarily).
- An example section would look like:

```
[mawnqc_dbh11]
host = 12.34.56.78
port = 5432
dbname = mawndb_qc
user = mydbusername
password = mydbpassword
```

> When running on your own computer, you may use an SSH tunnel to connect to the Enviroweather database server, and the host may be `127.0.0.1` and port the one used by the tunnel. See the Enviroweather system administrator for connection details.

- Two of the database entries are required for testing. You can either create these on a remote server or run a PostgreSQL server on your local computer. Create empty databases using the schema file for the `mawn_qc` database.

## How to run the hourly_main.py script (project entry script)

The entry script to the project is the `hourly_main.py` located in the `main_hourly_scripts` folder.

The following commands are used to run the `hourly_main.py` entry file that fetches data from one database and inserts/updates it into another:

```
cd ewx_utils/main_hourly_scripts

python hourly_main.py -a -x
python hourly_main.py --begin 2024-02-03 --end 2024-02-08 -a -x
python hourly_main.py --begin 2023-02-01 --end 2023-02-02 --station aetna -x

hourly_main [-h] [-b BEGIN] [-e END] [-f] [-c] (-x | -d) [-l] [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}] [--rtma {rtma:dbh11}]
```

### Additional Commands

```
hourly_main [-h] [-b BEGIN] [-e END] (-x | -d) [-s [STATIONS ...] | -a] --read-from READ_FROM [READ_FROM ...] --write-to WRITE_TO

python hourly_main.py -x -s aetna --read-from sample_section01 sample_section02 --write-to sample_section03

python hourly_main.py -x -b 2025-02-08 -e 2025-02-14 -a --read-from sample_section sample_section01 --write-to sample_section02

python hourly_main.py -x -s aetna --read-from mawn_dbh11 rtma_dbh11 --write-to mawnqc_test
```

### Database Configuration Requirements

1. **MAWN Database Section:**
   - The section name in `database.ini` MUST contain the characters 'mawn' (lowercase).
   - This is a strict requirement - the code specifically checks for 'mawn' in the section name.
   - Examples of valid section names:
     * `[mawn]`
     * `[mawn_database]`
     * `[mawn_prod]`
   - Examples of invalid section names:
     * `[MAWN]`
     * `[Mawn_db]`
     * `[weather_station]`

2. **RTMA Database Section:**
   - The section name in `database.ini` MUST contain the characters 'rtma' (lowercase).
   - This is a strict requirement - the code specifically checks for 'rtma' in the section name.
   - Examples of valid section names:
     * `[rtma]`
     * `[rtma_database]`
     * `[rtma_prod]`
   - Examples of invalid section names:
     * `[RTMA]`
     * `[Rtma_db]`
     * `[weather_grid]`

## How to run the hourly_utility.py script

- The main utility script for the ewx_utils project is the `hourly_utility.py` script.
- This script is used to compare records in the source database to the records in the destination database.
- A test database is used to mimic the destination database - records from the source database are stored here and compared to the destination database.
- Mismatches observed are stored in CSV files for easier comparisons.
- This script helps to ensure data accuracy and consistency across the board and provides an easy way to narrow down problematic values in our database as well as identifying bugs in the project code.
- Below is the command used to run the main utility script as well as the required arguments:

```
python hourly_utility.py --begin 2023-01-01 --end 2023-01-02 --station aetna_hourly

usage: hourly_utility.py [-h] -b BEGIN -e END -s STATION
required: -b/--begin, -e/--end, -s/--station
```

## How to run the clear_records.py script

- The `clear_records.py` script is also a utility script created to be used for dry-run purposes only.
- During the code testing phase, this script is useful in clearing/deleting faulty records in order to later reinsert clean records into a clean database.
- This script is purely for testing and dry-run purposes.
- Below is the command used to run the `clear_records.py` script as well as the required arguments:

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
