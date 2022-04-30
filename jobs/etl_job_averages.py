import os
from pyspark.sql.functions import *
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
    devices_df = extract_data(spark, config['data_lake_path'], config['device_table'])
    data_devices_df = extract_data(spark, config['data_lake_path'], config['data_device_table'])

    average_data_area_df, average_data_month_df = transform_data(devices_df, data_devices_df)

    load_data(average_data_area_df, config['data_lake_path'], 'averages_data_per_area')
    load_data(average_data_month_df, config['data_lake_path'], 'averages_data_per_month')

    # log the success and terminate Spark application
    log.warn('etl_job_devices is finished')
    spark.stop()
    return None


def transform_data(devices_df, data_devices_df):
    """Transform original dataset.

    :param devices_df: Input DataFrame, the device table.
    :param data_devices_df: Input DataFrame, the data device table.
    :return: Transformed DataFrames contained average quantities.
    """
    merged_data_devices_df = (data_devices_df
                              .join(devices_df, data_devices_df.device == devices_df.code, "inner")
                              .drop("code"))

    average_data_area_df = (merged_data_devices_df
                            .groupBy('area')
                            .agg(mean('CO2_level').alias('mean_CO2_level'),
                                 mean('humidity').alias('mean_humidity'),
                                 mean('temperature').alias('mean_temperature')))

    average_data_month_df = (merged_data_devices_df
                             .select(date_format('timestamp', 'yyyy-MM').alias('month'),
                                     col('CO2_level'), col('humidity'), col('temperature'))
                             .groupby('month')
                             .agg(mean('CO2_level').alias('mean_CO2_level'),
                                  mean('humidity').alias('mean_humidity'),
                                  mean('temperature').alias('mean_temperature')))

    return average_data_area_df, average_data_month_df


def extract_data(spark, data_lake_path, table_name):
    """Load data from csv file format.

    :param spark: Spark session object.
    :param data_lake_path: where path from where to retrieve the data
    :param table_name: name of the delta table
    :return: Spark DataFrame.
    """
    df = (spark.read.format("delta")
          .load(os.path.join(data_lake_path, table_name)))

    return df


def load_data(df, data_lake_path, table_name):
    """Collect data locally and write to Delta table.

    :param df: DataFrame to print.
    :param data_lake_path: path of the datalake, where delta tables are stored
    :param table_name: name of the table
    :return: None
    """
    (df.write.format('delta')
     .option("overwriteSchema", "true")
     .mode("overwrite")
     .save(os.path.join(data_lake_path, table_name)))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
