import os
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark


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
    load_data(devices_df=data,
              data_lake_path=config['data_lake_path'],
              partitioning_column=config['partitioning_column_devices'])

    # log the success and terminate Spark application
    log.warn('etl_job_devices is finished')
    spark.stop()
    return None


def extract_data(spark, landing_zone_path):
    """Load data from csv file format.

    :param spark: Spark session object.
    :param landing_zone_path: where path from where to retrieve the raw data
    :return: Spark DataFrame.
    """
    devices_df = spark.read.load(os.path.join(landing_zone_path, "devices/devices.csv"),
                                format='csv',
                                sep=',',
                                header='true',
                                inferSchema='true').cache()

    return devices_df


def load_data(devices_df, data_lake_path, partitioning_column):
    """Collect data locally and write to Delta table.

    :param devices_df: DataFrame to print.
    :param data_lake_path: path of the datalake, where delta tables are stored
    :param partitioning_column: column via which we partition the delta table
    :return: None
    """
    (devices_df.write.format('delta')
     .option("overwriteSchema", "true")
     .mode("overwrite").partitionBy(partitioning_column)
     .save(os.path.join(data_lake_path, 'raw_devices')))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
