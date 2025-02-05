from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Big Data Analysis") \
    .config("spark.driver.memory", "4g") \
    .master("local[*]") \
    .getOrCreate()


# Load Dataset (Example: NYC Taxi Trips dataset)
# Replace 'path_to_dataset.csv' with the actual dataset path
data_path = "path_to_dataset.csv"
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Display the schema of the dataset
data.printSchema()

# Show a sample of the data
data.show(5)

# Data Cleaning: Handle missing or null values
data = data.dropna()

# Exploratory Data Analysis (EDA)
# Example 1: Count the total number of records
record_count = data.count()
print(f"Total Records: {record_count}")

# Example 2: Find the average trip distance (if applicable)
if "trip_distance" in data.columns:
    avg_distance = data.select(avg(col("trip_distance"))).collect()[0][0]
    print(f"Average Trip Distance: {avg_distance} miles")

# Example 3: Count the number of trips per payment type (if applicable)
if "payment_type" in data.columns:
    payment_counts = data.groupBy("payment_type").count()
    payment_counts.show()

# Example 4: Find the longest and shortest trip distances
if "trip_distance" in data.columns:
    max_distance = data.select(max(col("trip_distance"))).collect()[0][0]
    min_distance = data.select(min(col("trip_distance"))).collect()[0][0]
    print(f"Longest Trip Distance: {max_distance} miles")
    print(f"Shortest Trip Distance: {min_distance} miles")

# # Advanced Analysis: Total revenue generated per day (if applicable)
# if "tpep_pickup_datetime" in data.columns and "total_amount" in data.columns:
#     data = data.withColumn("pickup_date", col("tpep_pickup_datetime").cast("date"))
#     revenue_per_day = data.groupBy("pickup_date").agg(sum(col("total_amount")).alias("total_revenue"))
#     revenue_per_day.orderBy("pickup_date").show()

# Stop the Spark session
spark.stop()
