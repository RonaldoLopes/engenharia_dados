import logging
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime, now

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

class BlackFridayTimeTable(Timetable):

    def is_black_friday(self, current_date: DateTime) -> bool:

        if current_date.month == 11 and current_date.weekday() == 4:
            last_day_of_november = current_date.end_of("month")
            return current_date.day > (last_day_of_november.day - 7)
        return False
    
    def next_dagrun_info(self, *, last_automated_data_interval, restriction) -> DagRunInfo:
        next_start = last_automated_data_interval.end if last_automated_data_interval else now()

        if self.is_black_friday(next_start):
            next_end = next_start.add(hours=1)
        else:
            next_start = next_start.start_of("day").add(hours=9)
            next_end = next_start.add(days=1)
        return DagRunInfo.interval(start=next_start, end=next_end)