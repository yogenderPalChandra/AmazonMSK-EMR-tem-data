"""

reading from the topic and doing some calculation on the data and writing to the csv file
"""


import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType,StringType, FloatType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import col

import boto3
from ec2_metadata import ec2_metadata

topic_input = "exp.tem1.spark.streaming"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")


def main():
    params = get_parameters()
    spark = SparkSession \
    .builder \
    .appName("kafka-batch-tem") \
    .getOrCreate()
    
    df_TH = read_from_kafka(spark, params)
    calculate_average_tem(df_TH, params)

def read_from_kafka(spark, params):
    
    options_read = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "subscribe":
            topic_input,
        "startingOffsets":
            "earliest",
        "endingOffsets":
            "latest",
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
    df_T = spark.read \
        .format("kafka") \
        .options(**options_read) \
        .load()


    return df_T

def calculate_average_tem(df_T, params):
    #path = os.getcwd()
    #path = "/home/yogender/Desktop/kafka/kafkaTemfiles"
    
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
    df_output = df_T \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn('Tem(Avg)', averageFunc) \
    
    df_output \
        .write \
        .format("console") \
        .option("numRows" ,50) \
        .option("truncate", False) \
        .save()

    #df_output.coalesce(1) \
    #    .write.format("csv") \
    #    .option("header", "false") \
    #    .save(os.path.join(path, "file2.txt"))

    df_output \
        .write \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark_output/tem_calculated",
             header=True, sep="|") \
        .mode("overwrite")

    #df_output \
    #    .write \
    #    .csv(path=path,
    #         header=True, sep="|") \
    #    .mode('append')

def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        "kafka_servers": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_servers")["Parameter"]["Value"],
        "kafka_demo_bucket": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_demo_bucket")["Parameter"]["Value"],
    }

    return params

if __name__=="__main__":
    main()



