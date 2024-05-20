import sys
from ewx_utils.validation_checks.mawndbsrc import clean_records
from ewx_utils.validation_checks.mawndbsrc import clean_record
from ewx_utils.validation_checks.qcsrc_cols import qc_columns
from ewx_utils.validation_checks.mawndbsrc import check_value
from ewx_utils.validation_checks.timeloop import generate_list_of_hours

for clean_record in clean_records: 
    key_clean = list(clean_record.keys()) 
    print(f"key_clean: {key_clean}") 
    print(len(key_clean))
    print(type(key_clean))
    val_clean = list(clean_record.values()) 
    print(f"val_clean: {val_clean}")
    print(type(val_clean))
    
"""
The above code loops through each clean_record in clean_records
It then converts the clean_record keys named key_clean into a list of keys to be used to determine which values to insert into mawndbqc
Further, it converts the clean_record values named val_clean into a list of values that will be inserted/transferred to mawndbqc

"""

qc_column_list = [item for sublist in qc_columns for item in sublist]
print(qc_column_list)
print(len(qc_column_list))


qc_clean_columns = []

for column in key_clean:
    if column in qc_column_list:
        qc_clean_columns.append(column)
print(f"qc_clean_columns: {qc_clean_columns}") 
print(len(qc_clean_columns))

"""
The code above creates a list of columns from qc_columns which are multiple tuples each tuple representing one column name.
List comprehension is used to perform the conversion of the multiple tuples and store them in a single list.
The list of columns is then put into a list called qc_column_list
Then an empty list of qc_clean_columns is initialized to store columns that key_clean has in common with qc_column_list
Then a loop is created to loop through the key_clean columns to find the columns that are also in qc_columns
Another part of the loop cross checks the qc_column_list with the columns in key_clean to find commonality.
Then each column that key_clean has in common with qc_column_list is appended into the qc_clean_columns list which was initialized in the beginning.

"""
