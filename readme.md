# EWX_UTILS Project

# The process of running this EWX_UTILS project in another machine
- The first step needed to run this project is to create SSH key through a tool such as PuTTYgen
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

## How to run the scripts
- The entry script to the project is the hourly_main.py located in the main_hourly_scripts folder.
- The following commands are be used to run the hourly_main.py entry file that fetches data from one databases and inserts/updates into another.
```
python hourly_main.py -a -x
python hourly_main.py --begin 2024-02-03 --end 2024-02-08 -a -x
python hourly_main.py --begin 2023-02-01 --end 2023-02-02 --station aetna -x

hourly_main [-h] [-b BEGIN] [-e END] [-f] [-c] (-x | -d) [-l] [-s [STATIONS ...] | -a]
[-q {mawnqc_test:local,mawnqcl:local,mawnqc:dbh11,mawnqc:supercell}] [--mawn {mawn:dbh11}] [--rtma {rtma:dbh11}]
```






