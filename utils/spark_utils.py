from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os
import re


def get_local_spark_session(app_name: str, conf: SparkConf = None) -> SparkSession:
    spark_conf = conf if conf else get_default_conf()
    spark = SparkSession.builder.appName(app_name).master("local[*]").config(conf=spark_conf).getOrCreate()
    return spark


def get_default_conf() -> SparkConf:
    conf = SparkConf()
    spark_config_path = os.getenv("SPARK_CONFIG_PATH")
    with open(spark_config_path, "r") as spark_config_file:
        for line in spark_config_file:
            if not line.startswith("#") and line.startswith("spark"):
                key, value = map(lambda x: x.strip(), re.split(r"\s+", line, maxsplit=1))
                conf.set(key, value)
    return conf
