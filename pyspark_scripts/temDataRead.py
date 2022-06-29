
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.window import Window

import boto3
from ec2_metadata import ec2_metadata

tem_data = "csvFile_2021_01_27.csv"
topic_output = "exp.tem1.spark.streaming"

os.environ['AWS_DEFAULT_REGION'] = ec2_metadata.region
ssm_client = boto3.client("ssm")




def main():

    params = get_parameters()
    spark = SparkSession.builder.appName("kafka-seed-tem").getOrCreate()
    df_T = read_csv(spark, params)
    write_to_kafka(params, df_T)

def read_csv(spark, params):

    
    #spark = SparkSession.builder.appName("kafka-seed-tem").getOrCreate()
    #path = "/home/yogender/Desktop/kafka/kafkaTemfiles/csvFile_2021_01_27.csv"
    

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
	

    #Two  line beneth are older version which run locally without AWS MSK
    #df = spark.read.csv(path=path,schema=schema, header=True, sep=",").drop("Unnamed: 0")
    #df_T.selectExpr("CAST(id AS STRING) AS key","to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "spark_topic_tem").save()

    df_T = spark.read \
               .csv(path=f"s3a://{params['kafka_demo_bucket']}/spark/{tem_data}",\
                    schema=schema, header=True, sep=",").drop("Unnamed: 0")
    return df_T

    

    

def write_to_kafka(params, df_T):
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

    df_T \
        .selectExpr("CAST(id AS STRING) AS key",
                    "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .options(**options_write) \
        .save()

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
