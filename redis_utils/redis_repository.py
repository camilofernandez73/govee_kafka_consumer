import time
import logging
import json
import redis
import datetime 

class RedisRepository:
    def __init__(self, redis_config):
        self.redis_client = redis.Redis(**redis_config)
        self.expiration_days = 2
        
        
    def set_latest(self, device_id, data):
        
        # Extract the relevant information from the data
        timestamp = data.get('timestamp')
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        battery = data.get('battery')

        current_utc_timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d %H:%M:%S')
        redis_key = f"device:{device_id}:latest"

        self.redis_client.hset(redis_key, mapping={
            "timestamp": timestamp,
            "temperature": temperature,
            "humidity": humidity,
            "battery": battery,
            "last_updated": current_utc_timestamp
        })

        logging.debug(f"Latest updated for {{device_id}}.  Key: '{redis_key}', Data: '{data}'")
        return True
 
    def set_history(self, device_id, data):
        
        # Extract the relevant information from the data
        timestamp = data.get('timestamp')
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        battery = data.get('battery')
        
        # Get the date from the timestamp
        date = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()

        if self._is_expired(date, self.expiration_days):
            logging.debug(f"History entry skipped date is old. Device {device_id} msg date: {date} is older than {self.expiration_days} days")
            return False

        # Generate the key for historical data
        historical_data_key = f"device:{device_id}:log:{date}"

        # Check if a reading with the same timestamp already exists
        existing_reading = self.redis_client.hget(historical_data_key, timestamp)

        if existing_reading:
            logging.debug(f"History entry skipped {device_id} not set ({timestamp})")
            return False 


        # Store the historical data in Redis with an expiration of 2 days
        self.redis_client.hset(historical_data_key, mapping={
            timestamp: json.dumps({
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "battery": battery
            })
        })
        self.redis_client.expire(historical_data_key, self.expiration_days * 24 * 60 * 60)  # Set expiration to self.expiration_days 

        logging.debug(f"History added for {device_id}. Key: '{historical_data_key}', Data: '{data}'")
        return True 

    def set_timeseries(self, device_id, data):
            
        # Extract the relevant information from the data
        timestamp = data.get('timestamp')
        temperature = data.get('temperature')
        humidity = data.get('humidity')
        battery = data.get('battery')

        # Get the date component from the timestamp
        date = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').date()
       
        if self._is_expired(date, self.expiration_days):
            logging.info(f"Time Series entry skipped date is old. Device {device_id} msg date: {date} is older than {self.expiration_days} days")
            return False

        # Get the time component from the timestamp
        time = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('%H:%M')

        # Round down the time to the nearest 5-minute interval
        interval_time = datetime.datetime.strptime(time, '%H:%M').replace(minute=(datetime.datetime.strptime(time, '%H:%M').minute // 5) * 5, second=0).strftime('%H:%M')

        # Generate the key for time series data
        time_series_key = f"device:{device_id}:timeseries:{date}"

        # Get the existing time series data for the interval
        existing_data = self.redis_client.hget(time_series_key, interval_time)

        if existing_data:
            # Parse the existing data
            existing_data = json.loads(existing_data)
            max_temp = max(existing_data['max_temp'], temperature)
            max_humidity = max(existing_data['max_humidity'], humidity)
            min_battery = min(existing_data['min_battery'], battery)
            total_readings = existing_data['total_readings'] + 1
        else:
            # Initialize the data for the interval
            max_temp = temperature
            max_humidity = humidity
            min_battery = battery
            total_readings = 1

        # Store the updated time series data in Redis with an expiration of 2 days
        self.redis_client.hset(time_series_key, mapping={
            interval_time: json.dumps({
                "datetime": f"{date} {interval_time}",
                "time": interval_time,
                "max_temp": max_temp,
                "max_humidity": max_humidity,
                "min_battery": min_battery,
                "total_readings": total_readings
            })
        })
        
        self.redis_client.expire(time_series_key, self.expiration_days * 24 * 60 * 60)  # Set expiration to self.expiration_days 

        return True
           
    def _is_expired(self, date, days):

        current_date = datetime.datetime.now(datetime.UTC).date()
        two_days_ago = current_date - datetime.timedelta(days=days)

        return date < two_days_ago

    def get_latest_timestamp(self, device_id):
        return self.redis_client.hget(f"device:{device_id}:latest", 'timestamp')
        
