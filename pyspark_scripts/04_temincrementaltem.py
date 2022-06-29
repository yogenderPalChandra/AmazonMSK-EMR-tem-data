

import os
import time
import boto3
from ec2_metadata import ec2_metadata

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

#path = "/home/yogender/Desktop/kafka/kafkaTemfiles/csv2.csv"
tem_data_second = "csv2.csv"

topic_output = "exp.tem1.spark.streaming"


time_between_messages = 0.5  # 1800 messages * .5 seconds = ~15 minutes
os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")




def main():

    params = get_parameters()


    spark = SparkSession \
        .builder \
        .appName("kafka-incremental-tem") \
        .getOrCreate()
    
    schema = StructType([StructField("Unnamed: 0", IntegerType(), False),\
	StructField("id", IntegerType(), False),StructField("dateTime", StringType(), False),\
	StructField("Tamb", FloatType(), False),StructField("TtopTestTankHPCir", FloatType(), False),StructField("TbottomTestTankHpCir", StringType(), False),\
	StructField("TtopSourceTank",  FloatType(), False), StructField("TloadTankMix",  FloatType(), False), StructField("TTopTestTankLoadCir",  FloatType(), False), \
        StructField("TloadMix",  FloatType(), False), StructField("TbottomSourceTank",  FloatType(), False),  StructField("TbottomTestTankLoadCir",  FloatType(), False), \
        StructField("T0",  FloatType(), False),StructField("T1",  FloatType(), False), StructField("T2",  FloatType(), False), StructField("T3",  FloatType(), False), \
        StructField("T4",  FloatType(), False), StructField("T5",  FloatType(), False), StructField("T6",  FloatType(), False), StructField("T7",  FloatType(), False), \
        StructField("T8",  FloatType(), False), StructField("T9",  FloatType(), False), StructField("flowHP",  FloatType(), False), StructField("flowLoad",  FloatType(), False), \
        StructField("Load_kW",  FloatType(), False), \
        StructField("Heat_Capacity_kW",  FloatType(), False)])
    df_TH = read_from_csv(spark,  schema, params)
    df_TH.cache()

    write_to_kafka(spark, df_TH, params)


def read_from_csv(spark, schema, params):
    df_T = spark.read \
        .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{tem_data_second}",
             schema=schema, header=True, sep=",")

    return df_T


def write_to_kafka(spark, df_T, params):

    options_write = {
        "kafka.bootstrap.servers":
            params["kafka_servers"],
        "topic":
            topic_output,
        "kafka.ssl.truststore.location":
            "/tmp/kafka.client.truststore.jks",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    }


    T_count = df_T.count()


    for r in range(0, T_count):
        row = df_T.collect()[r]
        df_message = spark.createDataFrame([row], df_T.schema)

        df_message = df_message \
            .drop("Unnamed: 0") \
            .selectExpr("CAST(id AS STRING) AS key",
                        "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()

        #df_message.show(1)

        time.sleep(time_between_messages)

def get_parameters():


    params = {
        "kafka_servers": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_servers")["Parameter"]["Value"],
        "kafka_demo_bucket": ssm_client.get_parameter(
            Name="/kafka_spark_demo/kafka_demo_bucket")["Parameter"]["Value"],
    }

    return params



if __name__ == "__main__":
    main()
