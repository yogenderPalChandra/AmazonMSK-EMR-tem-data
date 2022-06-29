

import os


import pyspark.sql.functions as F
import boto3
from ec2_metadata import ec2_metadata


from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType
from pyspark.sql.functions import col


topic_input = "exp.tem1.spark.streaming"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():

    params = get_parameters()

    spark = SparkSession \
        .builder \
        .appName("kafka-streaming-sales-console") \
        .getOrCreate()

    df_TH = read_from_kafka(spark, params)

    calculate_average_tem(df_TH)


def read_from_kafka(spark, params):
    '''This read the stream with readStream and finally later prints to the console
    '''

    options_read = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "subscribe":
            topic_input,
        "startingOffsets":
            "earliest",
        "kafka.ssl.truststore.location":
            "/tmp/kafka.client.truststore.jks",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }


    df_T = spark.readStream \
        .format("kafka") \
        .options(**options_read) \
        .load()

    return df_T


def calculate_average_tem(df_T):
    ''' This function reads the data from the topic still no average calculated,
    But calculates the strem read data (i.e. spark streaming) Tavg. 

    '''
    
    
    schema = StructType([StructField("id", IntegerType(), False),StructField("dateTime", StringType(), False),\
	StructField("Tamb", FloatType(), False),StructField("TtopTestTankHPCir", FloatType(), False),StructField("TbottomTestTankHpCir", StringType(), False),\
	StructField("TtopSourceTank",  FloatType(), False), StructField("TloadTankMix",  FloatType(), False), StructField("TTopTestTankLoadCir",  FloatType(), False), \
        StructField("TloadMix",  FloatType(), False), StructField("TbottomSourceTank",  FloatType(), False),  StructField("TbottomTestTankLoadCir",  FloatType(), False), \
        StructField("T0",  FloatType(), False),StructField("T1",  FloatType(), False), StructField("T2",  FloatType(), False), StructField("T3",  FloatType(), False), \
        StructField("T4",  FloatType(), False), StructField("T5",  FloatType(), False), StructField("T6",  FloatType(), False), StructField("T7",  FloatType(), False), \
        StructField("T8",  FloatType(), False), StructField("T9",  FloatType(), False), StructField("flowHP",  FloatType(), False), StructField("flowLoad",  FloatType(), False), \
        StructField("Load_kW",  FloatType(), False), StructField("Heat_Capacity_kW",  FloatType(), False)])

    T = [col('T0'), col('T1'), col('T2'), col('T3'), col('T4'),col('T5'),col('T6'),col('T7'),col('T8'),col('T9')]
    averageFunc = sum(x for x in T)/len(T)


    df_T = df_T \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn('Tem(Avg)', averageFunc) \
        .coalesce(1) \
        .writeStream \
        .queryName("streaming_to_console") \
        .trigger(processingTime="1 minute") \
        .outputMode("append") \
        .format("console") \
        .option("numRows", 25) \
        .option("truncate", False) \
        .start()

    df_T.awaitTermination()

def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        "kafka_servers": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_servers")["Parameter"]["Value"],
        "kafka_demo_bucket": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_demo_bucket")["Parameter"]["Value"],
    }

    return params



if __name__ == "__main__":
    main()
