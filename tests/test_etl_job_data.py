"""
test_etl_job_data.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job_data.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from pyspark.sql.functions import *
from pyspark.sql import Row

from dependencies.spark import start_spark
from jobs.etl_job_data import transform_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job_data.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark()
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def create_test_data(self):
        """Create test data.

        This function creates a small dataset to test the tranformation for the
         etl_job_data pipeline
        :return: PySpark DataFrame
        """

        # create example data from scratch
        local_records = [
            Row(CO2_level=903, device='6al7RTAobR', humidity=72,
                temperature=17,timestamp='2021-04-01T21:13:44.839Z', received='2021-04-04'),
            Row(CO2_level=903, device='8xUD6pzsQI', humidity=63,
                temperature=27, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=903, device='5gimpUrBB', humidity=53,
                temperature=15, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02')
        ]

        df = self.spark.createDataFrame(local_records)

    def test_transform_data_late_arrivals(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        input_records = [
            Row(CO2_level=903, device='6al7RTAobR', humidity=72,
                temperature=17, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-04'),
            Row(CO2_level=917, device='8xUD6pzsQI', humidity=63,
                temperature=27, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=1425, device='5gimpUrBB', humidity=53,
                temperature=15, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02')
        ]

        input_data = self.spark.createDataFrame(input_records)
        # assemble

        expected_records = [
            Row(CO2_level=917, device='8xUD6pzsQI', humidity=63,
                temperature=27, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02'),
            Row(CO2_level=1425, device='5gimpUrBB', humidity=53,
                temperature=15, timestamp='2021-04-01T21:13:44.839Z', received='2021-04-02')
        ]
        expected_data = self.spark.createDataFrame(expected_records)
        expected_data = expected_data.withColumn("timestamp", to_timestamp(expected_data.timestamp))

        transformed_data = transform_data(input_data)

        cols = len(transformed_data.columns)
        rows = transformed_data.count()

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in transformed_data.columns])

    def test_transform_data_drop_duplicates(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        input_records = [
            Row(CO2_level=828, device='14QL93sBR0j', humidity=72,
                temperature=10, timestamp='2021-04-04T00:18:45.481Z', received='2021-04-04'),
            Row(CO2_level=828, device='14QL93sBR0j', humidity=72,
                temperature=10, timestamp='2021-04-04T00:18:45.481Z', received='2021-04-04'),
            Row(CO2_level=1357, device='6al7RTAobR', humidity=25,
                temperature=30, timestamp='2021-04-04T17:03:58.972Z', received='2021-04-04'),
            Row(CO2_level=1005, device='2n2Pea', humidity=65,
                temperature=20, timestamp='2021-04-03T01:17:20.685Z', received='2021-04-03'),
            Row(CO2_level=1005, device='2n2Pea', humidity=65,
                temperature=20, timestamp='2021-04-03T01:17:20.685Z', received='2021-04-03'),
            Row(CO2_level=1581, device='2n2Pea', humidity=96,
                temperature=21, timestamp='2021-04-04T22:12:20.042Z', received='2021-04-04')
        ]

        input_data = self.spark.createDataFrame(input_records)
        # assemble

        expected_records = [
            Row(CO2_level=828, device='14QL93sBR0j', humidity=72,
                temperature=10, timestamp='2021-04-04T00:18:45.481Z', received='2021-04-04'),
            Row(CO2_level=1357, device='6al7RTAobR', humidity=25,
                temperature=30, timestamp='2021-04-04T17:03:58.972Z', received='2021-04-04'),
            Row(CO2_level=1005, device='2n2Pea', humidity=65,
                temperature=20, timestamp='2021-04-03T01:17:20.685Z', received='2021-04-03'),
            Row(CO2_level=1581, device='2n2Pea', humidity=96,
                temperature=21, timestamp='2021-04-04T22:12:20.042Z', received='2021-04-04')
        ]
        expected_data = self.spark.createDataFrame(expected_records)
        expected_data = expected_data.withColumn("timestamp", to_timestamp(expected_data.timestamp))

        transformed_data = transform_data(input_data)

        cols = len(transformed_data.columns)
        rows = transformed_data.count()

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in transformed_data.columns])


if __name__ == '__main__':
    unittest.main()
