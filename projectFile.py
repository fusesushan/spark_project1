from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, round, max, avg, sum, array, count, dense_rank
from pyspark.sql.functions import date_format, from_utc_timestamp, hour, min, year, month
from pyspark.sql.types import FloatType
import requests
from pyspark.sql.window import Window
from haversine import haversine, Unit

spark = SparkSession.builder.appName("projectFirst").getOrCreate()

fuel_prices_df = spark.read.parquet("data/cleaned_fuel_prices.parquet")
station_info_df = spark.read.parquet("data/cleaned_station_info.parquet")

def convert_to_euro(usd_price):
    # API URL to fetch the exchange rate
    api_url = "https://cdn.jsdelivr.net/gh/fawazahmed0/currency-api@1/latest/currencies/usd/eur.json"

    # Fetch exchange rate data from the API
    response = requests.get(api_url)
    exchange_rate_data = response.json()

    # Extract the exchange rate
    exchange_rate = exchange_rate_data["eur"]

    # Convert USD to Euro
    euro_price = usd_price * exchange_rate
    return euro_price

# Define a user-defined function (UDF) to calculate distances
@udf(FloatType())
def calculate_distance(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)


# Apply the UDF to convert prices from USD to Euro
fuel_prices_df = fuel_prices_df.withColumn("Price_Euro", convert_to_euro(col("Price")))
# Round the Euro prices to three decimal units
fuel_prices_df = fuel_prices_df.withColumn("Price_Euro_Rounded", round(col("Price_Euro"), 3))

# Drop the "Price_Euro" column
fuel_prices_df = fuel_prices_df.drop("Price_Euro")

# Show the updated DataFrame
fuel_prices_df.show()

# Find the maximum price
max_price = fuel_prices_df.agg(max("Price")).collect()[0][0]

# Find the ID with the highest price
highest_price_id = fuel_prices_df.filter(col("Price") == max_price).select("Id").first()[0]

# Order the data by price in descending order
sorted_prices_df = fuel_prices_df.orderBy(col("Price").desc())

# Calculate the average price
average_price = fuel_prices_df.agg(avg("Price")).collect()[0][0]

# Display the results
print("ID with Highest Price:", highest_price_id)
print("Maximum Average Price:", average_price)

fuel_prices_df = fuel_prices_df.withColumn("Day_of_Week", date_format(col("Date"), "E"))

day_sales_df = fuel_prices_df.groupBy("Day_of_Week").agg(count("*").alias("Sales_Count"))

# Order the data by sales count in descending order to rank the days
window_spec = Window.orderBy(col("Sales_Count").desc())
day_sales_ranked_df = day_sales_df.withColumn("Rank", dense_rank().over(window_spec))

day_sales_ranked_df.show()

station_info_df_copy = station_info_df.select(
    [col(col_name).alias(col_name + "_d2") for col_name in station_info_df.columns]
)

df_joined = station_info_df.crossJoin(station_info_df_copy)

# Convert string columns to float using the `cast` function
df_joined = df_joined.withColumn("Latitude", df_joined["Latitude"].cast(FloatType()))
df_joined = df_joined.withColumn("Longitudine", df_joined["Longitudine"].cast(FloatType()))
df_joined = df_joined.withColumn("Latitude_d2", df_joined["Latitude_d2"].cast(FloatType()))
df_joined = df_joined.withColumn("Longitudine_d2", df_joined["Longitudine_d2"].cast(FloatType()))

# Add a new column 'Distance_between' to the DataFrame using the UDF
result_df = df_joined.withColumn("Distance_between", calculate_distance(
    col("Latitude"), col("Longitudine"), col("Latitude_d2"), col("Longitudine_d2")
))

# Select the desired columns
result_df = result_df.select("Id", "Petrol_company", "Type", "Station_name", "City", "Id_d2", "Petrol_company_d2", "Type_d2", "Station_name_d2", "City_d2", "Distance_between")

# Show the resulting DataFrame
result_df.show(truncate=False)

# Filter the DataFrame to select stations within 100 kilometers
filtered_df = result_df.filter(result_df["Distance_between"] < 100)

# Show the filtered DataFrame
filtered_df.show(truncate=False)

# Extract the day of the week from the 'Date' column and create a new column 'Day_of_Week'
fuel_prices_df = fuel_prices_df.withColumn("Day_of_Week", date_format(col("Date"), "E"))

# Pivot the table to get counts for each day of the week
pivot_df = fuel_prices_df.groupBy().pivot("Day_of_Week").agg(count("*"))

# Display the result
pivot_df.show()

merged_df = fuel_prices_df.join(station_info_df , on="Id", how="inner")

merged_df.show()

# Extract the hour from the "Date" timestamp and add it as a new column "Hour"
merged_df = merged_df.withColumn("Hour", hour(merged_df["Date"]))

# Filter the data to include only self-service fuel stations in specific cities (Calcinate, Osio Sopra, Treviglio, Biella) and with Petrol_company Q8
cities_to_include = ["AGRIGENTO", "OSIO SOPRA"]
filtered_df = merged_df.filter((merged_df["isSelf"] == 1) & (merged_df["City"].isin(cities_to_include)) & (merged_df["Petrol_company"] == "Q8"))

print("Filtered Data Count:", filtered_df.count())

# Define a window specification to partition data by 'Petrol_company' and 'Hour'
window_spec = Window.partitionBy('Petrol_company', 'Hour')

# Calculate the total revenue for each group
total_revenue_df = filtered_df.withColumn('Total_Revenue', sum('Price').over(window_spec))

# Display the resulting DataFrame
total_revenue_df.show()

# Convert the timestamp to the specified timezone (America/New_York) and extract the hour
hour_df = merged_df.withColumn("Hour", hour(from_utc_timestamp(merged_df["Date"], "America/New_York")))

# Calculate hourly price fluctuations for each city
price_fluctuations_df = hour_df.groupBy("City", "Hour").agg((max("Price") - min("Price")).alias("Price_Fluctuation"))

# Calculate max and min prices separately
max_prices_df = hour_df.groupBy("City", "Hour").agg(max("Price").alias("Max_Price"))
min_prices_df = hour_df.groupBy("City", "Hour").agg(min("Price").alias("Min_Price"))

# Join max and min prices together
fluctuation_with_max_min_df = price_fluctuations_df.join(max_prices_df, ["City", "Hour"], "inner").join(min_prices_df, ["City", "Hour"], "inner")

# Create an array column combining max and min prices
price_array_df = fluctuation_with_max_min_df.withColumn("Prices", array("Max_Price", "Min_Price"))

# Find the top 10 cities with the highest price fluctuations
top_cities_fluctuations = price_array_df.groupBy("City").agg(avg("Price_Fluctuation").alias("Avg_Fluctuation")).orderBy("Avg_Fluctuation", ascending=False).limit(10)

# Join the DataFrames on the "City" column
result_df = top_cities_fluctuations.join(price_array_df, ["City"], "inner")

# Select the desired columns and order by "Avg_Fluctuation" in descending order
result_df = result_df.select("Prices", "Avg_Fluctuation", "City").distinct().orderBy(result_df["Avg_Fluctuation"].desc())

# Show the result
result_df.show(truncate=False)

# Define the window specification for the rolling average
rolling_window_spec = Window.partitionBy("City").orderBy("Date").rowsBetween(-6, 0)

# Calculate the rolling 7-hour average price
rolling_avg_price = filtered_df.withColumn("7_Hour_Avg_Price", avg("Price").over(rolling_window_spec))

# Determine the row with the maximum 7-hour average price within each city
max_avg_price_row = rolling_avg_price.withColumn("max_7_Hour_Avg_Price", max("7_Hour_Avg_Price").over(Window.partitionBy("City")))
result_df = max_avg_price_row.filter(col("7_Hour_Avg_Price") == col("max_7_Hour_Avg_Price")).drop("max_7_Hour_Avg_Price")

# Show the result
result_df.show()

# Calculate the average gasoline prices for each petrol company
avg_prices_df = merged_df.groupBy("Petrol_company").agg(avg("Price").alias("AveragePrice"))

# Sort the DataFrame in descending order of AveragePrice
sorted_avg_prices_df = avg_prices_df.orderBy(col("AveragePrice").desc())

# Show the result
sorted_avg_prices_df.show()

# Define the specific 'fuel station type' and 'city' to analyze
selected_fuel_station_type = 'Stradale'
selected_city = 'AGRIGENTO'

# Filter 'fuel_station_df' for the specific combination
filtered_fuel_station_df = station_info_df.filter((col('Type') == selected_fuel_station_type) & (col('City') == selected_city))

# Extract the month from the 'Date' column
joined_df = merged_df.withColumn('YearMonth', year(col('Date')) * 100 + month(col('Date')))

# Group by 'YearMonth' and find the minimum timestamp and minimum price within each group
min_date_price_per_month = joined_df.groupBy('YearMonth').agg(
    min('Date').cast('timestamp').alias('LowestRecordTimestamp'),
    min('Price').alias('LowestPrice')
)

# Find the month with the minimum timestamp and its corresponding lowest price
lowest_timestamp_month = min_date_price_per_month.orderBy('LowestRecordTimestamp').first()

# Display the result (if a lowest timestamp month is found)
if lowest_timestamp_month:
    print(f"The date with the lowest price for '{selected_fuel_station_type}' fuel stations in '{selected_city}' is: {lowest_timestamp_month['LowestRecordTimestamp']}")
    print(f"The lowest price for that month is: {lowest_timestamp_month['LowestPrice']}")
else:
    print(f"No data found for '{selected_fuel_station_type}' fuel stations in '{selected_city}'.")

# Stop the SparkSession when you're done
spark.stop()
