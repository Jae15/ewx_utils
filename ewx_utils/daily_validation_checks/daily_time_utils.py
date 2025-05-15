import os
import sys
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)

from ewx_utils.ewx_config import ewx_log_file
from datetime import datetime, timedelta
from dateutil.parser import parse
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple, Any

from ewx_utils.logs.ewx_utils_logs_config import ewx_unstructured_logger
from ewx_utils.logs.ewx_utils_logs_config import EWXStructuredLogger

my_validation_logger = EWXStructuredLogger(log_path=EWXStructuredLogger)

def generate_list_of_dates(begin_date: str, end_date: str) -> List:
    """
    Generate a list of date objects within a specified time range.

    This function creates a list of daily datetime objects between the given
    start and end dates, considering the America/Detroit timezone
    and accounting for daylight saving and time changes.

    Parameters:
        begin_date (str): The start date in string format.
        end_date (str): The end date in string format.
    
    Returns:
    list: A list of datetime objects representing each day in the specified range.
    """

    date_list = []

    try:
        # Parsing the input dates and setting timezone using ZoneInfo
        begin_date = parse(begin_date).replace(tzinfo=ZoneInfo("America/Detroit"))
        end_date = parse(end_date).replace(tzinfo=ZoneInfo("America/Detroit"))

        # Adjusting end_date to include the entire last day
        end_date = end_date + timedelta(days=1)

        # Considering the current datetime
        now = datetime.now(tz=ZoneInfo("America/Detroit"))

        this_date = begin_date

        # Generating daily datetime objects until the end date or current time
        while this_date < end_date and this_date.date() <= now.date():
            date_list.append(this_date)
            this_date += timedelta(days=1)

    except ValueError as e:
        my_validation_logger.error(f"Invalid date format: {e}")
    except Exception as e:
        my_validation_logger.error(f"An error occurred: {e}")

    return date_list

def generage_list_of_nth_dats(begin_date: str, end_date: str) -> list:
    """
    Generate a list of nth day numbers of the year, calculated as (nth day -1)

    Parameters:
        begin_date (str): The start date in string format.
        end_date (str): The end date in string format.

    Returns:
    list: A list of integers representing the nth day of the year (0-bases index).
    """
    nth_day_list = []

    try:
        # Get the list of dates using the function above
        date_list = generate_list_of_dates(begin_date, end_date)

        # Convert each date to its nth day of the year (0-based index)
        nth_day_list = [(date.timetuple().tm_yday - 1) for date in date_list]
    
    except Exception as e:
        my_validation_logger.error(f"An error occurred: {e}")

    return nth_day_list