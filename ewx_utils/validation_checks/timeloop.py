import logging
from datetime import datetime, timedelta
from dateutil.parser import parse
from dateutil import tz

def generate_list_of_hours(begin_date: str, end_date: str) -> list:
    """
    Generate a list of datetime objects within a specified time range by considering
    the existence of each datetime in the America/Detroit timezone, including daylight saving changes.
    """
    datetime_list = []

    try:
        """
        Parsing the input dates and set timezone
        - `parse(begin_date/end_date)` uses the `dateutil.parser` module to convert the `date` string into a `datetime` object.
        - `.replace(tzinfo=tz.gettz("America/Detroit"))` sets the timezone of the parsed `datetime` object to "America/Detroit".
        This ensures that the `begin_date_time` is aware of the specific timezone, which is important for handling daylight saving time correctly
        """
        begin_date_time = parse(begin_date).replace(tzinfo=tz.gettz("America/Detroit"))
        end_date_time = parse(end_date).replace(tzinfo=tz.gettz("America/Detroit"))

        # Adjusting end_date_time to be the next day
        end_date_time += timedelta(days=1)

        # Considering the current datetime
        now = datetime.now(tz=tz.gettz("America/Detroit"))

        this_date = begin_date_time

        while this_date < end_date_time and this_date <= now:

            datetime_list.append(this_date)
            this_date += timedelta(hours=1)

    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    return datetime_list