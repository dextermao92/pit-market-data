from airflow.plugins_manager import AirflowPlugin

from pit.airflow_timetable import MarketHoursTimetable


class MarketHoursPlugin(AirflowPlugin):
    name = "market_hours"
    timetables = [MarketHoursTimetable]
