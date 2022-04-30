#!/bin/bash

SPHOME/bin/spark-submit --packages io.delta:delta-core_2.12:1.2.1 --py-files packages.zip  --files  configs/etl_config.json \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
			    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
			    jobs/etl_job_averages.py
