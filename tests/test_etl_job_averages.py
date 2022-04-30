"""
test_etl_job_averages.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in test_etl_job_averages.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

from pyspark.sql.functions import *
from pyspark.sql import Row

from dependencies.spark import start_spark
from jobs.etl_job_averages import transform_data


class SparkETLAveragesTests(unittest.TestCase):
    """Test suite for transformation in etl_job_data.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, *_ = start_spark()

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data_averages(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        data_input_records = [
            Row(CO2_level=903, device='8xUD6pzsQI', humidity=72,
                temperature=17, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=727, device='14QL93sBR0j', humidity=63,
                temperature=27, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=947, device='36TWSKiT', humidity=63,
                temperature=18, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=917, device='6al7RTAobR', humidity=63,
                temperature=30, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=1425, device='1xbYRYcj', humidity=53,
                temperature=15, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02')
        ]

        input_data_df = self.spark.createDataFrame(data_input_records)

        device_input_records = [
            Row(code='8xUD6pzsQI', type='capacitive', area='commercial', customer='AB-Service'),
            Row(code='14QL93sBR0j', type='resistive', area='commercial', customer='Atlanta Group'),
            Row(code='36TWSKiT', type='thermal-conductivity', area='residential', customer='Net-Free'),
            Row(code='6al7RTAobR', type='capacitive', area='residential', customer='Atlanta Group'),
            Row(code='1xbYRYcj', type='resistive', area='industrial', customer='AB-Service')
        ]

        input_device_df = self.spark.createDataFrame(device_input_records)
        averages_area, averages_month = transform_data(input_device_df, input_data_df)
        area_cols = len(averages_area.columns)
        area_rows = averages_area.count()
        month_cols = len(averages_month.columns)
        month_rows = averages_month.count()

        expected_averages_area_records = [
            Row(area='commercial', mean_CO2_level=815.0, mean_humidity=67.5,mean_temperature=22.0),
            Row(area='residential', mean_CO2_level=932.0, mean_humidity=63.0, mean_temperature=24.0),
            Row(area='industrial', mean_CO2_level=1425.0, mean_humidity=53.0, mean_temperature=15.0),
        ]
        expected_averages_area = self.spark.createDataFrame(expected_averages_area_records)

        expected_averages_month_records = [
            Row(month='2021-04', mean_CO2_level=983.8, mean_humidity=62.8, mean_temperature=21.4)
        ]
        expected_averages_month = self.spark.createDataFrame(expected_averages_month_records)
        expected_averages_month = (expected_averages_month
                                   .withColumn("month", (to_date(expected_averages_month.month, 'yyyy-MM'))))
        expected_averages_month = (expected_averages_month
                                   .withColumn("month", (date_format(col('month'), 'yyyy-MM'))))
        expected_averages_month.show()
        averages_month.show()
        expected_area_cols = len(expected_averages_area.columns)
        expected_area_rows = expected_averages_area.count()

        expected_month_cols = len(expected_averages_month.columns)
        expected_month_rows = expected_averages_month.count()

        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1_area = [*map(field_list, averages_area.schema.fields)]
        fields2_area_exptected = [*map(field_list, expected_averages_area.schema.fields)]
        fields1_month = [*map(field_list, averages_month.schema.fields)]
        fields2_month_exptected = [*map(field_list, expected_averages_month.schema.fields)]

        res_area = set(fields1_area) == set(fields2_area_exptected)
        res_month = set(fields1_month) == set(fields2_month_exptected)

        # assert
        self.assertTrue(res_area)
        self.assertTrue(res_month)
        self.assertEqual(expected_area_cols, area_cols)
        self.assertEqual(expected_area_rows, area_rows)
        self.assertEqual(expected_month_cols, month_cols)
        self.assertEqual(expected_month_rows, month_rows)

        self.assertTrue([col in expected_averages_month.columns
                         for col in averages_month.columns])

        self.assertTrue([col in expected_averages_area.columns
                         for col in averages_area.columns])


if __name__ == '__main__':
    unittest.main()
