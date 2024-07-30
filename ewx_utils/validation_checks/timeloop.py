import sys

sys.path.append("c:/Users/mwangija/data_file/ewx_utils/ewx_utils")
import logging
from datetime import datetime, timedelta
import tz

# from dateutil import tz
from validation_logsconfig import validations_logger

validation_logger = validations_logger()


def generate_list_of_hours(begin_date: str, end_date: str) -> list:
    """
    generate_list_of_hours(2022-5-5, 2022-5-6)
    The function generate_list_of_hours is defined above and takes in two parameters - a begin_date and an end_date.
    It is used to generate a list of datetime objects within a specified time range by considering the existence of each datetime in a specific timezone.
    This is achieved using the successive code chunks below that help iterate through each day and the hours of each day.
    The function generate_list_of_hours takes in two dates in form of strings which are stored in a list i.e begin_date and end_date and then returns
    a list of datetime objects that are valid in the America/Detroit timezone.
    The time increments are done day by day which are then broken down hour by hour - daylight saving is also taken care of in the spring and fall seasons.
    The datetime objects are stored in a list called datetime_list
    """

    datetime_list = []

    begin_date_time = datetime(
        int(begin_date[0:4]),
        int(begin_date[5:7]),
        int(begin_date[8:10]),
        tzinfo=tz.gettz("America/Detroit"),
    )

    end_date_time = datetime(
        int(end_date[0:4]),
        int(end_date[5:7]),
        int(end_date[8:10]),
        tzinfo=tz.gettz("America/Detroit"),
    )

    end_date_time = end_date_time + timedelta(days=1)

    this_date = begin_date_time

    while end_date_time >= this_date and this_date < datetime.now(
        tz=tz.gettz("America/Detroit")
    ):
        if tz.datetime_exists(this_date, tz.gettz("America/Detroit")):
            datetime_list.append(this_date)
        this_date = this_date + timedelta(hours=1)
    return datetime_list


"""
The above while loop does the following: 
1. `if tz.datetime_exists(this_date, tz.gettz("America/Detroit")):` - This line checks if the current datetime `this_date` exists in the specified time zone "America/Detroit" using the `datetime_exists` function from the `dateutil.tz` module.

2. `datetime_list.append(this_date)` - If the datetime exists, it adds the current datetime `this_date` to the list `datetime_list`.

3. `this_date = this_date + timedelta(hours=1)` - Regardless of the condition, this line increments the `this_date` by one hour using the `timedelta` function from the `datetime` module, ensuring the loop progresses to the next hour.

4. Finally, the when the function is executed, it returns a list of datetime objects named `datetime_list` to the caller.

"""
