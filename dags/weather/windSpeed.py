import requests
import csv
from datetime import datetime
from weather.weatherBase import WeatherBase
from weather.urls import WIND_SPEED_URL

class WindSpeed(WeatherBase):
    def __init__(self, url):
        super().__init__(url)

    def retrieve_data(self) -> dict:
        t_s = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        r_o = requests.get(self.url, data = {"date_time": t_s})
        if r_o.status_code == 200:
            json_data = r_o.json()
            locations_list = json_data.get("metadata").get("stations")
            units = json_data.get("metadata").get("reading_unit")
            sensor_recorded_time = json_data.get("items")[0].get("timestamp")
            readings_list = json_data.get("items")[0].get("readings")
            return self.grab_fields(readings_list, locations_list, units, sensor_recorded_time, r_o.status_code)
        else:
            return {"status_code": r_o.status_code}

    @staticmethod
    def grab_fields(readings_list : list, locations_list : list , units : str, sensor_recorded_time : str, status_code: int):
        result = {"status_code": status_code}
        for a,b in zip(readings_list, locations_list):
            location = b["name"]
            if location is not None:
                result[location] = dict(
                    timestamp = sensor_recorded_time,
                    location = location,
                    longitude = b.get("location").get("longitude"),
                    latitude = b.get("location").get("latitude"),
                    station_id = a.get("station_id"),
                    value = a.get("value"),
                    units = units
                )
        return result

    def store_data(self):
        result = self.retrieve_data()
        entries = []
        fields = []
        with open("/opt/airflow/files/windSpeed.csv", "w", encoding="utf-8", newline="") as file:
            for key, value in result.items():
                if key != "status_code":
                    entries.append(value)
                    fields = value.keys()
            writer = csv.DictWriter(file, fieldnames = fields)
            writer.writeheader()
            writer.writerows(entries)