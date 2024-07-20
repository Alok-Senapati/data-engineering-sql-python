from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, count, when, round
from utils.spark_utils import get_local_spark_session


def main():
    spark = get_local_spark_session("user_trips_analysis")

    users_df = spark.read.csv("../data/users", header=True)
    trips_df = spark.read.csv("../data/trips", header=True)
    users_df.cache()
    trips_df.cache()

    unbanned_users_df = users_df.filter(col("banned") == lit("No"))
    unbanned_users_df.cache()

    users_df.createOrReplaceTempView("users")
    trips_df.createOrReplaceTempView("trips")

    cancellation_rate_df = trips_df.filter(col("request_at").between("2023-10-01", "2023-10-11")).alias("t") \
        .join(broadcast(unbanned_users_df.filter(col("role") == lit("driver"))).alias("ub_drivers"),
              col("t.driver_id") == col("ub_drivers.user_id")) \
        .join(broadcast(unbanned_users_df.filter(col("role") == lit("client"))).alias("ub_clients"),
              col("t.client_id") == col("ub_clients.user_id")) \
        .select(col("t.request_at"), col("t.client_id"), col("t.driver_id"), col("t.status")) \
        .groupBy(col("request_at")) \
        .agg(
        count("status").alias("total_trips"),
        count(when(col("status") != lit("completed"), True)).alias("total_cancelled_trips")
    ) \
        .select("request_at",
                round(col("total_cancelled_trips") * lit(100) / col("total_trips"), 2).alias("cancellation_rate"))

    cancellation_rate_sql_df = spark.sql(
        """
            SELECT t.request_at, ROUND(COUNT(CASE WHEN status != 'completed' THEN 1 END) * 100 / COUNT(status), 2) AS cancelled_rate
            FROM trips t 
                INNER JOIN (SELECT user_id FROM users WHERE role = 'client' AND banned = 'No') ubc ON t.client_id = ubc.user_id
                INNER JOIN (SELECT user_id FROM users WHERE role = 'driver' AND banned = 'No') ubd ON t.driver_id = ubd.user_id
            WHERE request_at BETWEEN '2023-10-01' AND '2023-10-11'
            GROUP BY t.request_at
        """
    )

    non_matching_count = cancellation_rate_df.join(cancellation_rate_sql_df,
                                                   cancellation_rate_df["request_at"] == cancellation_rate_sql_df[
                                                       "request_at"], "anti")
    cancellation_rate_df.write.csv("../data/ut_analysis", mode="overwrite", header=True)
    non_matching_count.write.csv("../data/ut_val", mode="overwrite", header=True)


if __name__ == '__main__':
    main()
