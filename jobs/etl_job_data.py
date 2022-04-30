import os
from dependencies.spark import start_spark
from pyspark.sql.functions import *


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='etl_job',
        files=['configs/etl_config.json'],
        jar_packages='io.delta:delta-core_2.12:1.2.1',
        spark_config={'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                      'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog'})

    # log that main ETL job is starting
    log.warn('etl_job_devices is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark, config['landing_zone_path'])
    load_raw_data(data, config['data_lake_path'])
    transformed_data = transform_data(data)
    load_cleansed_data(data_df=transformed_data,
                       data_lake_path=config['data_lake_path'],
                       partitioning_column=config['partitioning_column_data_devices'])

    # log the success and terminate Spark application
    log.warn('etl_job_devices is finished')
    spark.stop()
    return None


def extract_data(spark, landing_zone_path):
    """Load data from json file format.

    :param spark: Spark session object.
    :param landing_zone_path: where path from where to retrieve the raw data
    :return: Spark DataFrame.
    """
    data_path = os.path.join(landing_zone_path, 'data')
    data_df = spark.read \
        .option("basePath", data_path) \
        .json(os.path.join(landing_zone_path, 'data/*/*.json'))

    return data_df


def transform_data(data_df):
    """Transform original dataset.

    :param data_df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    transformed_data_df = data_df.withColumn("timestamp", to_timestamp(data_df.timestamp))
    transformed_data_df = transformed_data_df.where(datediff(col('received'), col('timestamp')) <= 1)
    transformed_data_df = transformed_data_df.dropDuplicates(['device', 'timestamp'])

    return transformed_data_df


def load_cleansed_data(data_df, data_lake_path, partitioning_column):
    """Collect data locally and write to Delta table.

    :param data_df: DataFrame to print.
    :param data_lake_path: path of the datalake, where delta tables are stored
    :param partitioning_column: column via which we partition the delta table
    :return: None
    """
    (data_df.write
        .option("overwriteSchema", "true")
        .partitionBy('timestamp')
        .format('delta')
        .mode("overwrite")
        .save(os.path.join(data_lake_path, 'cleansed_data_devices')))

    return None


def load_raw_data(data_df, data_lake_path):
    """Collect data locally and write to Delta table.

    :param data_df: DataFrame to print.
    :param data_lake_path: path of the datalake, where delta tables are stored
    :return: None
    """
    (data_df.write.format('delta')
     .mode("overwrite")
     .save(os.path.join(data_lake_path, 'raw_data_devices')))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
