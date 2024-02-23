import os
import uuid
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer
import simplejson as json
import random

LONDON_COORDINATES = {
    "lat": 51.509865,
    "lon": -0.118092
}
BIRMINGHAM_COORDINATES = {
    "lat": 52.486243,
    "lon": -1.890401
}

# calculate movement increment
LATITUDE_INCREMENT = (LONDON_COORDINATES["lat"] - BIRMINGHAM_COORDINATES["lat"]) / 100
LONGITUDINAL_INCREMENT = (LONDON_COORDINATES["lon"] - BIRMINGHAM_COORDINATES["lon"]) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=1)
    return start_time


def simulate_vehicle_movement():
    global start_location
    start_location["lat"] += LATITUDE_INCREMENT
    start_location["lon"] += LONGITUDINAL_INCREMENT

    start_location['lat'] += random.uniform(-0.0005, 0.0005)
    start_location['lon'] += random.uniform(-0.0005, 0.0005)

    return start_location
def generate_emergency_incident_data(vehicle_id, timestamp,location):
    return{
        'id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'vehicle_id': vehicle_id,
        'incident_id': str(uuid.uuid4()),
        'location': location,
        'type': random.choice(['accident', 'fire', 'flood']),
        'status': random.choice(['reported', 'in_progress', 'resolved']),
        'description': 'A serious accident has occurred on the road',
       

    }
def generate_gps_data(vehicle_id, timestamp,vehicle_type ='private'):
    return{
        'id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'vehicle_id': vehicle_id,
        'speed': random.uniform(10, 40),  # km/h
        'direction': 'North-East',
        'location': (start_location['lat'], start_location['lon']),
        'vehicle_type': vehicle_type
     }
def generate_weather_data(vehicle_id, timestamp,location):
    return{
        'id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'vehicle_id': vehicle_id,
        'location': location,
        'temperature': random.uniform(10, 40),  # Celsius
        'humidity': random.uniform(0, 100),  # percentage
        'precipitation': random.uniform(0, 100),  # percentage
        'wind_speed': random.uniform(0, 100),  # km/h
        'wind_direction': 'North-East',

    }
def generate_traffic_camera_data(vehicle_id, timestamp,location,camera_id):
    return{
        'id': str(uuid.uuid4()),
        'timestamp': timestamp,
        'vehicle_id': vehicle_id,
        'camera_id': camera_id,
        'snapshot': 'Base64EncodedString',
        'location': location
    }
def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['lat'], location['lon']),
        'speed': random.uniform(10, 40),  # km/h
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2019,
        'fuelType': 'Hybrid'
    }


    


def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        traffic_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'],vehicle_data['location'], camera_id='camera-123')
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'],vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'],vehicle_data['location'])
        print(vehicle_data)
        print(gps_data)
        print(traffic_data)
        print(weather_data)
        print(emergency_incident_data)

        break
        # Send data to Kafka topics here using the producer


if __name__ == "__main__":
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f"kafka Error: {err}"),
    }
    producer = SerializingProducer(producer_conf)
    try:
        simulate_journey(producer, 'vehicle-123')
    except KeyboardInterrupt:
        print('Simulation ended by user')
    except Exception as e:
        print(f"An error occurred: {e}")
