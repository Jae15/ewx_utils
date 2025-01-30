#!/usr/bin/env python
import os
import sys
import decimal
import argparse
import csv
import dotenv
from datetime import date

dotenv.load_dotenv()
from dotenv import load_dotenv

load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from datetime import datetime
from ewx_utils.ewx_config import ewx_log_file
from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger
from ewx_utils.db_files.dbs_connections import (
    connect_to_mawnqc_test,
    connect_to_mawnqc_supercell,
    mawnqc_test_cursor_connection,
    mawn_supercell_cursor_connection,
)

my_logger = ewx_utils_logger(log_path=ewx_log_file)


def fetch_records_by_date(cursor, station, start_date, end_date):
    """
    Fetch records from the specified database table for a given date range.

    Parameters:
        cursor: Database cursor object to execute queries.
        station (str): Name of the table (station) to query.
        start_date (str): Start date of the query in YYYY-MM-DD format.
        end_date (str): End date of the query in YYYY-MM-DD format.

    Returns:
        list: A list of dictionaries containing the fetched records.

    Raises:
        Exception: If the query fails or another error occurs.
    """
    query = f"""
    SELECT * FROM {station}
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    my_logger.error(f"Executing query: {query}")
    try:
        cursor.execute(query)
        records = cursor.fetchall()
        my_logger.error(
            f"Fetched {len(records)} records from {station} for date range {start_date} to {end_date}."
        )
        return [dict(record) for record in records]
    except Exception as e:
        my_logger.error(f"Error fetching records from {station}: {e}")
        raise


def limit_to_max_digits(num, max_digits=None):
    """
    Limit the number of digits in a decimal number to a specified maximum.

    Parameters:
        num (float or None): The input number to be rounded.
        max_digits (int, optional): Maximum number of digits. Defaults to 6.

    Returns:
        decimal.Decimal or None: The rounded decimal number, or None if an error occurs.

    Logs:
        Errors and unexpected conditions are logged.
    """
    try:
        if num is None:
            my_logger.error("Input num is None.")
            return None

        if max_digits is None:
            max_digits = 6

        if num == 0:
            return decimal.Decimal("0.0")

        num_decimal = decimal.Decimal(str(num))
        
        integer_part = num_decimal.to_integral_value()
        #print(f"Integer part type: {type(integer_part)}")
        if integer_part == 0:
            integer_digits = 0
        else:
            integer_digits = len(str(integer_part))
        decimal_places = max_digits - integer_digits

        if decimal_places < 0:
            decimal_places = 0
        #print(f"Decimal Places: {decimal_places}")

        quantize_str = decimal.Decimal("1." + "0" * decimal_places)

        rounded_decimal = num_decimal.quantize(
            quantize_str, rounding=decimal.ROUND_HALF_UP
        )
        return rounded_decimal

    except decimal.ConversionSyntax as e:
        my_logger.error(
            f"ConversionSyntax error for input num: {num}, max_digits: {max_digits}. Error: {e}"
        )
        return None
    except Exception as e:
        my_logger.error(
            f"An unexpected error occurred for input num: {num}, max_digits: {max_digits}. Error: {e}"
        )
        return None


def is_within_margin(value1, value2):
    """
    Check if the difference between two numbers is within a 0.05% margin of the second number.

    Parameters:
        value1 (float or str): First value to compare.
        value2 (float or str): Second value to compare.

    Returns:
        bool: True if within margin, False otherwise.

    Logs:
        Information about the comparison and writes details to a CSV file.
    """
    try:
        if value1 is None or value2 is None:
            my_logger.error(
                f"One of the values is None: value1: {value1}, value2: {value2}."
            )
            return False

        # Converting both values to Decimal
        try:
            value1_decimal = decimal.Decimal(value1)
            value2_decimal = decimal.Decimal(value2)
        except (ValueError, TypeError) as e:
            my_logger.error(
                f"Non-numeric values provided: value1: {value1}, value2: {value2}. Error: {e}"
            )
            return False

        # Calculating the margin as 0.05% of value2
        margin = abs(value2_decimal) * decimal.Decimal("0.0005")  # 0.05% of value2

        my_logger.info(
            f"Comparing value1: {value1_decimal} with value2: {value2_decimal}. Calculated margin: {margin}"
        )

        result = abs(value1_decimal - value2_decimal) <= margin
        my_logger.info(f"Result of comparison: {result}")

        # Append the types and values to a CSV file in the specified directory
        csv_file_path = (
            r"C:\Users\mwangija\data_file\ewx_utils\ewx_utils\comparison_results.csv"
        )

        # Check if the file exists to write headers only once
        file_exists = os.path.isfile(csv_file_path)

        with open(csv_file_path, mode="a", newline="") as file:
            writer = csv.writer(file)

            # Write headers if the file does not exist
            if not file_exists:
                writer.writerow(
                    [
                        "Type of Value1",
                        "Value1",
                        "Type of Value2",
                        "Value2",
                        "Type of Value1 Decimal",
                        "Value1 Decimal",
                        "Type of Value2 Decimal",
                        "Value2 Decimal",
                        "Margin",
                        "Result",
                    ]
                )

            writer.writerow(
                [
                    type(value1).__name__,
                    value1,
                    type(value2).__name__,
                    value2,
                    type(value1_decimal).__name__,
                    value1_decimal,
                    type(value2_decimal).__name__,
                    value2_decimal,
                    margin,
                    result,
                ]
            )

        return result

    except Exception as e:
        my_logger.error(
            f"An error occurred while comparing values {value1} and {value2}. Error: {e}"
        )
        return False


def compare_records(test_records, supercell_records):
    """
    Compare records between test and supercell databases and identify mismatches or missing records.

    Parameters:
        test_records (list): Records from the test database.
        supercell_records (list): Records from the supercell database.

    Returns:
        tuple: Lists of records only in test, only in supercell, and mismatched details.

    Logs:
        Mismatched details and writes them to CSV files.
    """

    test_records_dict = {(rec["date"], rec["time"]): rec for rec in test_records}
    supercell_records_dict = {
        (rec["date"], rec["time"]): rec for rec in supercell_records
    }
    only_in_test = []
    only_in_supercell = []
    mismatches_details = []

    # Specify the full path for the CSV files
    srad_csv_file_path = (
        r"C:\Users\mwangija\data_file\ewx_utils\ewx_utils\srad_values.csv"
    )
    general_mismatch_csv_file_path = (
        r"C:\Users\mwangija\data_file\ewx_utils\ewx_utils\general_mismatches.csv"
    )

    # Check if the srad file exists to write headers only once
    srad_file_exists = os.path.isfile(srad_csv_file_path)
    general_file_exists = os.path.isfile(general_mismatch_csv_file_path)

    # Open the CSV file for srad values
    with open(srad_csv_file_path, "w", newline="") as srad_file:
        writer = csv.writer(srad_file)

        # Write headers if the file does not exist
        if not srad_file_exists:
            writer.writerow(["Test Value", "Supercell Value"])

        for key in test_records_dict:
            if key not in supercell_records_dict:
                only_in_test.append(test_records_dict[key])
            else:
                # Excluding 'id' from the records for comparison
                test_record = {
                    k: v for k, v in test_records_dict[key].items() if k != "id"
                }
                supercell_record = {
                    k: v for k, v in supercell_records_dict[key].items() if k != "id"
                }
                # Extracting the year using the get method
                year = test_record.get("year")
                record_date = test_record["date"]
                # print(f"record_date_type:{type(record_date)}")
                start_of_vapr = date(2024, 9, 1)
                # print(f"start_of_vapr: {type(start_of_vapr)}")print(type(f"record_date_type:{record_date}"))

                for column_name in test_record.keys():
                    if column_name in supercell_record:
                        # For years before 2017, we skip comparison for '_src' and 'volt' columns
                        if year < 2017:
                            if column_name.endswith("_src") or column_name == "volt":
                                continue
                        if test_record[column_name] == -7999 and test_record.get(f"{column_name}_src") == "OOR":
                            if supercell_record[column_name] is None and supercell_record.get(f"{column_name}_src") == "OOR":
                                continue
                        columns = ['stmp_05cm_src', 'stmp_10cm_src', 'stmp_20cm_src', 'stmp_50cm_src', 
                                   'smst_05cm_src', 'smst_10cm_src', 'smst_20cm_src', 'smst_50cm_src']
                        if column_name in columns:
                            if (
                                test_record[column_name] == "EMPTY"
                                and supercell_record[column_name] is None
                                ):
                                continue
                        poly_cols = ["polyatmp_src", "polystmp1_src", "polystmp2_src"]
                        if column_name in poly_cols:
                            if(
                              test_record[column_name] == "EMPTY"
                              and supercell_records[column_name] == "OOR"  
                            ):
                                continue

                        # Defining conditions for relh when the value has been capped to 100 and source set to RELH_CAP
                        if column_name == "relh_src":
                            if (
                            test_record[column_name] == "RELH_CAP"
                            and supercell_record[column_name] == "MAWN"
                        ):
                                continue
                        if column_name == "relh":
                            if (test_record[column_name] == 100
                                and 100 <= supercell_record[column_name] <= 105
                            ):
                                continue
                        if column_name == "relh_src":
                            if (test_record[column_name] == "OOR"
                                and supercell_record[column_name] == "EMPTY"
                            ):
                                continue
                        # Defining conditions for dwpt and dwpt_src where the values have been replaced from RTMA
                        if column_name == "dwpt":
                            if supercell_record[column_name] is None:
                                continue
                            if (
                                test_record[column_name] is not None
                                and supercell_record[column_name] is None
                            ):
                                continue
                        if column_name == "dwpt_src":
                            if (
                                test_record[column_name] is not None
                                and supercell_record[column_name] == "EMPTY" 
                                or supercell_record[column_name] == "OOR"
                            ):
                                continue
                        if column_name == "dwpt_src":
                                if(
                                    test_record[column_name] == "RTMA"
                                    and supercell_record[column_name] is None
                                ):
                                    continue
                        wstdv_cols = ["wstdv_src", "wstdv_20m_src"]
                        if column_name in wstdv_cols:
                            if (
                                test_record[column_name] == "EMPTY"
                                and supercell_record[column_name] == "OOR"
                            ):
                                continue
                        if column_name == "volt_src":
                            if(
                                test_record[column_name] == "EMPTY"
                                and supercell_record[column_name] == "OOR"
                            ):
                                continue
                        # Defining conditions for the srad values
                        limit_cols = ["srad", "relh", "soil0", "soil1", "atmp"]
                        if column_name in limit_cols:
                            test_value = test_record[column_name]
                            supercell_value = supercell_record[column_name]
                            test_source = test_record.get(f"{column_name}_src")
                            supercell_source = supercell_record.get(
                                f"{column_name}_src"
                            )
                            if (
                                test_value is None
                                and test_source == "EMPTY"
                                or test_source == "OOR"
                                and supercell_value is None
                                and supercell_source == "EMPTY"
                                or supercell_source == "OOR"
                            ):
                                continue
                            #print(f"hour: {test_record['hour']}")
                            #print(f"Test Value before: {test_value}")
                            #print(f"Supercell Value before: {supercell_value}")

                            test_value = limit_to_max_digits(test_record[column_name])
                            supercell_value = limit_to_max_digits(
                                supercell_record[column_name]
                            )
                            #print(f"Test Value after limtomax: {test_value}")
                            #print(f"Supercell Value after limtomax: {supercell_value}")
                            if not is_within_margin(test_value, supercell_value):
                                mismatches_details.append(
                                    [test_record, supercell_record, column_name]
                                )
                                #print(f"Test Value after withmargn: {test_value}")
                                #print(f"Supercell Value after withmargn: {supercell_value}")
                                writer.writerow([test_value, supercell_value])
                            

                        # Defining conditions for the relh values
                        elif (
                            column_name == "relh"
                            and test_record.get("relh_src") == "RELH_CAP"
                        ):
                            if not (100 < test_record[column_name] <= 105):
                                if (
                                    test_record[column_name]
                                    != supercell_record[column_name]
                                ):
                                    mismatches_details.append(
                                        [test_record, supercell_record, column_name]
                                    )
                                    # Write to general mismatches CSV
                                    with open(
                                        general_mismatch_csv_file_path, "a", newline=""
                                    ) as general_file:
                                        general_writer = csv.writer(general_file)
                                        if not general_file_exists:
                                            general_writer.writerow(
                                                [
                                                    "Date",
                                                    "Time",
                                                    "Column Name",
                                                    "Test Value",
                                                    "Supercell Value",
                                                ]
                                            )
                                        general_writer.writerow(
                                            [
                                                test_record["date"],
                                                test_record["time"],
                                                column_name,
                                                test_record[column_name],
                                                supercell_record[column_name],
                                            ]
                                        )

                        elif column_name == "vapr" and record_date < date(2024, 9, 1):
                            if column_name.endswith("_src"):
                                continue
                        elif column_name in [
                            "vapr_src",
                            "vapr_3m_src",
                            "vapr_45cm_src",
                        ]:
                            if (
                                test_record[column_name] == "EMPTY"
                                and supercell_record[column_name] == None
                            ):
                                continue

                        else:
                            # Comparing values for all other columns
                            if (
                                test_record[column_name]
                                != supercell_record[column_name]
                            ):
                                mismatches_details.append(
                                    [test_record, supercell_record, column_name]
                                )
                                # Write to general mismatches CSV
                                with open(
                                    general_mismatch_csv_file_path, "a", newline=""
                                ) as general_file:
                                    general_writer = csv.writer(general_file)
                                    if not general_file_exists:
                                        general_writer.writerow(
                                            [
                                                "Date",
                                                "Time",
                                                "Column Name",
                                                "Test Value",
                                                "Supercell Value",
                                            ]
                                        )
                                    general_writer.writerow(
                                        [
                                            test_record["date"],
                                            test_record["time"],
                                            column_name,
                                            test_record[column_name],
                                            supercell_record[column_name],
                                        ]
                                    )

    for key in supercell_records_dict:
        if key not in test_records_dict:
            only_in_supercell.append(supercell_records_dict[key])

    return only_in_test, only_in_supercell, mismatches_details


def main():
    parser = argparse.ArgumentParser(
        description="Utility script to compare records between test and supercell databases"
    )
    parser.add_argument(
        "-b", "--begin", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "-e", "--end", type=str, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "-s",
        "--station",
        type=str,
        required=True,
        help="Station name (which is also the table name)",
    )

    args = parser.parse_args()

    try:
        # Create database connections and cursors
        test_conn = connect_to_mawnqc_test()
        test_cursor = mawnqc_test_cursor_connection(test_conn)

        supercell_conn = connect_to_mawnqc_supercell()
        supercell_cursor = mawn_supercell_cursor_connection(supercell_conn)

        # Fetch records from both databases
        test_records = fetch_records_by_date(
            test_cursor, args.station, args.begin, args.end
        )
        supercell_records = fetch_records_by_date(
            supercell_cursor, args.station, args.begin, args.end
        )

        # Compare records
        only_in_test, only_in_supercell, mismatches_details = compare_records(
            test_records, supercell_records
        )
        # my_logger.error(type(only_in_test))
        # my_logger.error(type(only_in_supercell))
        # my_logger.error(type(mismatches_details))

        # Report results
        if only_in_test:
            my_logger.error(f"Records found only in test database: {len(only_in_test)}")
            #print(f"Records found only in test database: {len(only_in_test)}")
        if only_in_supercell:
            my_logger.error(
                f"Records found only in supercell database: {len(only_in_supercell)}"
            )
            #print(f"Records found only in supercell database: {len(only_in_supercell)}")
        if mismatches_details:
            my_logger.error(f"Mismatched records: {len(mismatches_details)}")
            # my_logger.error(type(mismatches_details))
            my_logger.error(mismatches_details)
            for mismatch in mismatches_details:
                my_logger.error(f"Test Record : {mismatch[0]}")
                #print(f"Test Record Pre Truncation/Rounding: {mismatch[0]}")
                my_logger.error(f"Supercell Record: {mismatch[1]}")
                #print(f"Supercell Record: {mismatch[1]}")
                my_logger.error(f"Details: {mismatch[2]}")
                #print(f"Details: {mismatch[2]}")

    except Exception as e:
        my_logger.error(f"An error occurred: {e}")
    finally:
        # Close connections
        test_conn.close()
        supercell_conn.close()


if __name__ == "__main__":
    main()


"""
python hourly_utility.py --begin 2023-01-01 --end 2023-01-02 --station aetna_hourly

"""
