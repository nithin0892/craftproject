import sys
import logging
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, concat, lit, when
from pyspark.sql.types import DoubleType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def glue_etl():
    try:
        # Job config
        JOB_NAME = "ETLab" 
        input_path = "s3://traderesults/raw_data/output_csv_full.csv" 
        output_path = "s3://traderesults/transformed-data/"          
        logger.info(f"Job started: {JOB_NAME}")
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(JOB_NAME, {'JOB_NAME': JOB_NAME})
        logger.info(f"Reading data from {input_path}")
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [input_path], "recurse": True},
            format="csv",
            format_options={"withHeader": True, "separator": ","}
        )
        df = dynamic_frame.toDF()
        required_columns = ['time_ref', 'value']
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        logger.info("Inspecting raw 'time_ref' column values...")
        #Display sample records
        df.select("time_ref").distinct().show(10, truncate=False)
        logger.info("Transforming 'time_ref' column to date format...")
        #Converting to data format 
        df = df.withColumn(
            "time_ref",
            when(col("time_ref").rlike("^[0-9]{6}$"), to_date(concat(col("time_ref"), lit("01")), "yyyyMMdd"))
            .otherwise(None)  
        )
        logger.info("Transforming 'value' column to numeric format...")
        df = df.withColumn(
            "value",
            when(col("value").rlike("^[0-9]+(\\.[0-9]*)?$"), col("value").cast(DoubleType()))
            .otherwise(None)  
        )
        logger.info("Inspecting transformed data...")
        df.select("time_ref", "value").show(10, truncate=False)
        logger.info("Converting DataFrame back to DynamicFrame...")
        dynamic_frame_updated = DynamicFrame.fromDF(df, glueContext, "dynamic_frame_updated")
        logger.info(f"Writing transformed data to {output_path}")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame_updated,
            connection_type="s3",
            connection_options={"path": output_path, "partitionKeys": ["time_ref"]},
            format="parquet"
        )
        job.commit()
        logger.info("ETL Transformation completed and saved to S3.")
    except Exception as e:
        logger.exception("Error in Glue ETL job")
        raise
    finally:
        sc.stop()

if __name__ == "__main__":
    glue_etl()
