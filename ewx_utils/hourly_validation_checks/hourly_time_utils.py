import os
import sys
import dotenv

dotenv.load_dotenv()
from dotenv import load_dotenv

load_dotenv()
ewx_base_path = os.getenv("EWX_BASE_PATH")
sys.path.append(ewx_base_path)
from ewx_utils.ewx_config import ewx_log_file
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil import tz

from ewx_utils.logs.ewx_utils_logs_config import ewx_utils_logger

# Initialize the logger
my_validation_logger = ewx_utils_logger(log_path=ewx_log_file)


def generate_list_of_hours(begin_date: str, end_date: str) -> list:
    """
    Generate a list of datetime objects within a specified time range.

    This function creates a list of hourly datetime objects between the given
    start and end dates, considering the America/Detroit timezone and
    accounting for daylight saving time changes.

    Parameters:
    begin_date (str): The start date in string format.
    end_date (str): The end date in string format.

    Returns:
    list: A list of datetime objects representing each hour in the specified range.
        Parsing the input dates and set timezone
        - `parse(begin_date/end_date)` uses the `dateutil.parser` module to convert the `date` string into a `datetime` object.
        - `.replace(tzinfo=tz.gettz("America/Detroit"))` sets the timezone of the parsed `datetime` object to "America/Detroit".
        This ensures that the `begin_date_time` is aware of the specific timezone, which is important for handling daylight saving time correctly

    """
    datetime_list = []

    try:
        # Parsing the input dates and setting timezone
        begin_date_time = parse(begin_date).replace(tzinfo=tz.gettz("America/Detroit"))
        end_date_time = parse(end_date).replace(tzinfo=tz.gettz("America/Detroit"))

        # Adjusting end_date_time to include the entire last day
        end_date_time += timedelta(days=1)

        # Considering the current datetime
        now = datetime.now(tz=tz.gettz("America/Detroit"))

        this_date = begin_date_time

        # Generating hourly datetime objects until the end date or current time
        while this_date < end_date_time and this_date <= now:
            datetime_list.append(this_date)
            this_date += timedelta(hours=1)

    except ValueError as e:
        my_validation_logger.error(f"Invalid date format: {e}")
    except Exception as e:
        my_validation_logger.error(f"An error occurred: {e}")

    return datetime_list



