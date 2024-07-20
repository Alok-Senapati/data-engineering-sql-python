import random
import datetime
from faker import Faker
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

num_users = 400000
num_trips = 2000000
banned_statuses = ['Yes', 'No']
roles = ['client', 'driver', 'partner']
status_options = ['completed', 'cancelled_by_driver', 'cancelled_by_client']
start_date = datetime.datetime(2023, 1, 1)
end_date = datetime.datetime(2024, 7, 1)


def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())))


fake = Faker()

users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("user_name", StringType(), False),
    StructField("banned", StringType(), False),
    StructField("role", StringType(), False),
])
trips_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("client_id", IntegerType(), False),
    StructField("driver_id", IntegerType(), False),
    StructField("city_id", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("request_at", StringType(), False),
    StructField("payment", DoubleType(), False)
])


spark = (SparkSession.builder.appName("user_trips_datagen").master("local[*]")
         .config("spark.hadoop.dfs.client.write.create_checksum", "false").getOrCreate())
users_df = spark.createDataFrame(
    [
        (i, fake.name(), random.choice(banned_statuses), random.choice(roles))
        for i in range(1, num_users + 1)
    ],
    schema=users_schema
).repartition(10000).persist(StorageLevel.MEMORY_AND_DISK)
driver_ids = list(map(
    lambda x: x.asDict()["user_id"], users_df.filter(col("role") == lit("driver")).select(col("user_id")).rdd.collect()
))
client_ids = list(map(
    lambda x: x.asDict()["user_id"], users_df.filter(col("role") == lit("client")).select(col("user_id")).rdd.collect()
))

trips_df = spark.createDataFrame(
    [
        (i, random.choice(client_ids), random.choice(driver_ids), random.randint(1, 50), random.choice(status_options),
         random_date(start_date, end_date).strftime("%Y-%m-%d"), round(random.uniform(50, 2000), 2)) for i in range(1, num_trips)
    ],
    schema=trips_schema
).persist(StorageLevel.MEMORY_AND_DISK)

users_df.repartition(1000).write.csv(
    "../data/users", mode='overwrite', header=True
)

trips_df.repartition(2000).write.csv(
    "../data/trips", mode='overwrite', header=True
)

