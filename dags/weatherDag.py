from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from pendulum import TRANSITION_ERROR
from weather.humidity import Humidity
from weather.rainfall import RainFall
from weather.airTemperature import AirTemperature
from weather.windDirection import WindDirection
from weather.windSpeed import WindSpeed
from airflow.utils.trigger_rule import TriggerRule
from weather.urls import RAINFALL_URL, TEMPERATURE_URL, HUMIDITY_URL, WIND_SPEED_URL, WIND_DIRECTION_URL


default_args = {
    "owner" : "Airflow",
    "start_date": datetime(2022,1,23),
    "retries" : 2, 
    "retry_delay" : timedelta(seconds=5)
}

def downloading_air_temperature(url = TEMPERATURE_URL):
    at = AirTemperature(url)
    at.store_data()

def downloading_rainfall(url = RAINFALL_URL):
    ar = RainFall(url)
    ar.store_data()

def downloading_humidity(url = HUMIDITY_URL):
    ah = Humidity(url)
    ah.store_data()

def downloading_wind_direction(url = WIND_DIRECTION_URL):
    wd = WindDirection(url)
    wd.store_data()

def downloading_wind_speed(url = WIND_SPEED_URL):
    wd = WindSpeed(url)
    wd.store_data()

def insert_data(tableName, fileName, **kwargs):
    header = []
    records = []
    destination_hook = PostgresHook(postgres_conn_id='external_psql_conn', schema='airflow')
    destination_conn = destination_hook.get_conn()
    destination_cursor = destination_conn.cursor()
    with open(f"/opt/airflow/files/{fileName}.csv", "r") as f:
        for line in f:
            if len(header) == 0:
                header = line.split(',')
                continue
            value = line.strip().split(',')
            records.append(value)
        if len(records):
            kk = tuple([tuple(i) for i in records])
            execute_values(destination_cursor, f"INSERT INTO airflow.{tableName} VALUES %s", kk)
    destination_conn.commit()
    destination_cursor.close()
    destination_conn.close()

            
        
with DAG("WeatherTasks", default_args=default_args, schedule_interval="*/5 * * * *", catchup=False) as dag:
    is_temperature_api_available = HttpSensor(
        task_id = "isTemperatureApiAvailable",
        http_conn_id = 'sg_weather_temperature_api',
        endpoint = "",
        response_check=lambda response: "metadata" in response.text,
        poke_interval = 5,
        timeout = 20 
    )

    collect_temperature_data = PythonOperator(
        task_id = "collectLiveTemperatureData", 
        python_callable = downloading_air_temperature,
        
    ) 

    insert_temperature_data = PythonOperator(
        task_id = "insertLiveTemperatureData",
        python_callable = insert_data,
        op_kwargs = {"tableName": "sg_weather_temperature", "fileName": "air_temperature"}
    )

    is_rainfall_api_available = HttpSensor(
        task_id = "isRainfallApiAvailable",
        http_conn_id = "sg_weather_rainfall_api",
        endpoint = "",
        response_check=lambda response: "metadata" in response.text,
        poke_interval = 5,
        timeout = 20
    )

    collect_rainfall_data = PythonOperator(
        task_id = "collectLiveRainFallData", 
        python_callable = downloading_rainfall,
    ) 

    insert_rainfall_data = PythonOperator(
        task_id = "insertLiveRainFallData",
        python_callable = insert_data,
        op_kwargs = {"tableName": "sg_weather_rainfall", "fileName": "rainfall"}
    )

    is_humidity_api_available = HttpSensor(
        task_id = "isHumidityApiAvailable",
        http_conn_id = "sg_weather_humidity_api",
        endpoint = "",
        response_check=lambda response: "metadata" in response.text,
        poke_interval = 5,
        timeout = 20
    )

    collect_humidity_data = PythonOperator(
        task_id = "collectLiveHumidityData", 
        python_callable = downloading_humidity,
    ) 

    insert_humidity_data = PythonOperator(
        task_id = "insertHumidityData",
        python_callable = insert_data,
        op_kwargs = {"tableName": "sg_weather_humidity", "fileName": "humidity"}
    )

    is_wind_direction_api_available = HttpSensor(
        task_id = "isWindDirectionApiAvailable",
        http_conn_id = "sg_weather_winddir_api",
        endpoint = "",
        response_check=lambda response: "metadata" in response.text,
        poke_interval = 5,
        timeout = 20
    )

    collect_wind_direction = PythonOperator(
        task_id = "collectWindSpeedData", 
        python_callable = downloading_wind_direction,
    ) 

    insert_wind_direction_data = PythonOperator(
        task_id = "insertWindDirectionData",
        python_callable = insert_data,
        op_kwargs = {"tableName": "sg_weather_wind_direction", "fileName": "windDirection"}
    )

    is_wind_speed_api_available = HttpSensor(
        task_id = "isWindSpeedApiAvailable",
        http_conn_id = "sg_weather_windspd_api",
        endpoint = "",
        response_check=lambda response: "metadata" in response.text,
        poke_interval = 5,
        timeout = 20
    )

    collect_wind_speed = PythonOperator(
        task_id = "collectLiveWindSpeed", 
        python_callable = downloading_wind_speed,
    ) 

    insert_wind_speed_data = PythonOperator(
        task_id = "insertWindSpeedData",
        python_callable = insert_data,
        op_kwargs = {"tableName": "sg_weather_wind_speed", "fileName": "windSpeed"}
    )

    download_join = DummyOperator(
        task_id = "download_summary",
        trigger_rule=  TriggerRule.ONE_FAILED
    )

    ready_for_update = DummyOperator(
        task_id = "ready_for_insert",
        trigger_rule=  TriggerRule.ALL_SUCCESS
    )

    insert_join = DummyOperator(
        task_id = "insert_summary",
        trigger_rule=  TriggerRule.ONE_FAILED
    )

    # update_db_join = DummyOperator(
    #     task_id = "insert_db_summary",
    #     trigger_rule=  TriggerRule.ONE_FAILED
    # )

    download_notification = EmailOperator(task_id='send_download_mail',
        to='alan.leeyungchong@gmail.com',
        subject='Singapore Weather API downloaded',
        html_content=""" <h4>One of the weather api has failed: please check Airflow</h4> """)


    insert_db_notification = EmailOperator(task_id='send_db_mail',
        to='alan.leeyungchong@gmail.com',
        subject='Singapore Weather API inserted into DB',
        html_content=""" <h4>Insert of Data into the data base has failed, please check airflow.</h4> """)

    is_temperature_api_available >> collect_temperature_data 
    is_rainfall_api_available >> collect_rainfall_data
    is_humidity_api_available >> collect_humidity_data
    is_wind_direction_api_available >> collect_wind_direction
    is_wind_speed_api_available >> collect_wind_speed 

    [collect_temperature_data, collect_rainfall_data,collect_humidity_data, collect_wind_direction,  collect_wind_speed] >> download_join >> download_notification
    [collect_temperature_data, collect_rainfall_data,collect_humidity_data, collect_wind_direction,  collect_wind_speed] >> ready_for_update 
    ready_for_update >> [insert_humidity_data, insert_rainfall_data, insert_temperature_data, insert_wind_direction_data, insert_wind_speed_data] >> insert_join
    insert_join >> insert_db_notification