# Fuel Price Data Analysis with PySpark

This project is a data analysis and processing pipeline implemented using PySpark. It performs various data operations on fuel price and station information data, providing valuable insights into fuel pricing and station locations.


## Data Sources

The project uses the following data sources:
The link to dataset used :
https://www.kaggle.com/datasets/alessandrolobello/gasoline-hourly-price-tracker-from-2022?select=Hourly_Gasoline_Prices.csv
### Fuel Station Information (fuel_station_information.csv)

This dataset contains information about fuel stations, including their managers, petrol companies, types, station names, cities, and coordinates.

- **Id**: Unique identifier for the fuel station.
- **Fuel_station_manager**: Manager of the fuel station.
- **Petrol_company**: Name of the petrol company.
- **Type**: Type of fuel station.
- **Station_name**: Name of the fuel station.
- **City**: City where the fuel station is located.
- **Latitude**: Latitude coordinates of the fuel station.
- **Longitudine**: Longitude coordinates of the fuel station.

### Hourly Gasoline Price (hourly_gasoline_price.csv)

This dataset contains hourly gasoline prices for different fuel stations.

- **Id**: Unique identifier for the fuel station.
- **isSelf**: Indicator for self-service (1 for self-service, 0 for not self-service).
- **Price**: Gasoline price in USD.
- **Date**: Date and time of the price measurement.


## Overview

The project includes the following key functionalities:

- **Currency Conversion**: Convert fuel prices from USD to Euro using an API for exchange rates.
- **Data Analysis**: Analyze fuel prices, calculate distances between stations, rank sales by day of the week, and identify cities within a 100km radius.
- **Business Insights**: Derive insights such as total revenue by petrol company, cities with the highest price fluctuations, and more.

## Requirements

To run this project, you need the following Python packages:

appdirs==1.4.3
haversine==2.8.0
requests==2.22.0
findspark==2.0.1

You can install these packages using the following command:
`pip install -r requirements.txt`

## Requirements
To get started with the project, follow these steps:

    1. Clone the repository to your local machine.
    2. Install the required dependencies using the `pip install -r requirements.txt` command.
    3. Run the `spark-submit projectFile.py` to perform data analysis and processing.
