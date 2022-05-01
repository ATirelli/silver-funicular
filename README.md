# PySpark Example Project

This repository contains the code implementing an ETL pipeline using Apacke Spark 
and its Python API, PySpark. In what follows we shall provide details regarding: 

- the structure of the repository
- how to build all necessary dependendencies to run the ETL jobs
- how to run the three implemented ETL jobs
- how to run unit tests
- possible improvements to be implemented in the future

In order to showcase the functionalities of the pipeline we created a `data` folder, representing the storage
area where data are extracted from and loaded to. Such source and destination paths can be set in the `configs/config.json`
folder.

## ETL Project Structure
The project structure is the following:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- data/
 |  |-- datalake/
 |  |-- landing_zone/
 |-- dependencies/
 |   |-- __init__.py
 |   |-- logging.py
 |   |-- spark.py
 |-- jobs/
 |   |-- etl_job_averages.py
 |   |-- etl_job_data.py
 |   |-- etl_job_devices.py
 |-- tests/
 |   |-- test_etl_job_averages.py
 |   |-- test_etl_job_data.py
 |-- build_dependencies.sh
 |-- packages.zip
 |-- Pipfile
 |-- Pipfile.lock
```

The main Python modules containing the ETL jobs (which will be sent to the Spark cluster), 
are contined in the `jobs` folder. Any external configuration parameters required by any of the ETL jobs in `jobs`
 are stored in JSON format in `configs/etl_config.json`. 
Additional modules that support this job are in the `dependencies` folder 
(more on this later). In the project's root we include `build_dependencies.sh`, 
which is a bash script for building these dependencies into a zip-file to be 
sent to the cluster (`packages.zip`). Unit test modules are kept in the `tests` folder.

## Structure of the ETL Jobs

In order to facilitate easy debugging and testing, we isolated the 'Transformation' step be isolated from the 'Extract' 
and 'Load' steps, into its own function - taking input data arguments in the form of DataFrames and returning the 
transformed data as a single DataFrame. Then, the code that surrounds the use of 
the transformation function in the `main()` job function, is concerned with 
Extracting the data, passing it to the transformation function and then Loading 
(or writing) the results to their ultimate destination. 


## Passing Configuration Parameters to the ETL Job

In order pass arguments to any of the ETL jobs in the `jobs` folder, we use the 
`--files configs/etl_config.json` flag with `spark-submit` - containing the configuration in JSON format, which can be parsed into a 
Python dictionary in one line of code with `json.loads(config_file_contents)`. 

For the exact details of how the configuration file is located, opened and parsed, please see the `start_spark()` function in `dependencies/spark.py` (also discussed further below), which in addition to parsing the configuration file sent to Spark (and returning it as a Python dictionary), also launches the Spark driver program (the application) on the cluster and retrieves the Spark logger at the same time.

## Packaging ETL Job Dependencies

In this project, functions that can be used across different ETL jobs are kept in a module called `dependencies` and referenced in specific job modules using, for example,

```python
from dependencies.spark import start_spark
```

This package, together with any additional dependencies referenced within it, must be copied to each Spark node for all jobs that use `dependencies` to run. This is achieved by sending all dependencies as a `zip` archive together with the job, using `--py-files` with Spark submit. In order to produce the `.zip` file we use the `build_dependencies.sh` bash script for automating the production of `packages.zip`, given a list of dependencies documented in `Pipfile` and managed by the `pipenv` python application (discussed below).

## Running the ETL jobs

The various ETL jobs contained in the `jobs` folder can be launched via the corresponding `bash` scripts, whose general form is the following:

```bash
#!/bin/bash
PYSPARKHOME=<here you should put the path to yuor SPARK home>
$PYSPARKHOME/bin/spark-submit --packages io.delta:delta-core_2.12:1.2.1 --py-files packages.zip  --files  configs/etl_config.json \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
jobs/etl_job_data.py
```

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--files configs/etl_config.json` - the path to any config file that may be required by the ETL job;
- `--py-files packages.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `jobs/etl_job_data.py` - the Python module file containing the ETL job to execute.


## Automated Testing


Given that we have chosen to structure our ETL jobs in such a way as to isolate the 'Transformation' step into its own function, we feed it a small slice of 'real-world' production data that is generated on-the-fly and check it against known results (e.g. computed manually or interactively within a Python interactive console session).

To execute the example unit test for this project run the `bash` script `launch_tests.sh`.

## Possible Improvements
Some possible improvements of the present implementations are as follows:
- we tried to make the jobs as parametrized as possible, but of course further modularity could be implemented, for example designing commond `extract` `load` functions accross the different jobs that are able to automatically detect the type of data sources (`.csv` and `.json` in our case).
- continuous integration should be added to the project
