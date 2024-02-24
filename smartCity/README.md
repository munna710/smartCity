![](https://github.com/munna710/smartCity/blob/main/smartCity/images/systemArchitecure.png)
## Overview
`SmartCity` is a data engineering project aimed at leveraging big data technologies to improve urban life. The project collects, processes, and analyzes various types of data from city infrastructure, vehicles, weather stations, and more. The insights derived from this data can help city planners make informed decisions, improve public services, and enhance the quality of life for residents.

## System Architecture
The system architecture is as follows:

1. **Data Inputs**: The system collects various types of data inputs like vehicle information, GPS information, camera information, weather information, and emergency information.
2. **Data Streaming**: The collected data is streamed using Docker and Kafka.
3. **Data Processing**: Apache Spark is used for processing the streamed data.
4. **Data Storage**: The processed data is stored using AWS services like Data Loading and Amazon Redshift.
5. **Data Visualization**: The stored data is visualized using Tableau and Looker Studio.
## Data Generation
The project generates simulated data for various aspects of a smart city:

- **Vehicle Data**: Information about vehicles, including their location, speed, direction, make, model, year, and fuel type.
- **GPS Data**: GPS data for each vehicle, including speed, direction, and location.
- **Weather Data**: Weather conditions at each vehicle's location, including temperature, humidity, precipitation, wind speed, and wind direction.
- **Emergency Incident Data**: Information about emergency incidents, including the type of incident (accident, fire, flood), status (reported, in progress, resolved), and a description.
- **Traffic Camera Data**: Data from traffic cameras, including a snapshot (represented as a Base64 encoded string) and the camera's location.

The data is generated in real-time, with the location of each vehicle changing over time to simulate movement.

## Data Streaming and Processing
The generated data is streamed to Kafka topics for each type of data (vehicle data, GPS data, weather data, emergency data, and traffic camera data). A Confluent Kafka producer is used to produce the data to the Kafka topics. The data is then processed using Apache Spark.
