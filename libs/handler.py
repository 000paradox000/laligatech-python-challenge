from datetime import datetime, date
from pathlib import Path
import re
import warnings

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import trim, from_unixtime, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


class Handler:
    def __init__(self):
        self._setup()

        self.base_dir = Path(__file__).resolve().parent.parent
        self.files_dir = self.base_dir / "files"
        self.input_files_dir = self.files_dir / "input"

        self.df_using_rows = None
        self.df_using_schema = None
        self.df_using_pandas = None
        self.df_using_rdd = None
        self.df_using_csv = None
        self.df_using_log = None

        self.spark = SparkSession.builder.getOrCreate()

    def go(self):
        print("")
        # self.create_df_using_rows()
        # self.create_df_using_schema()
        # self.create_df_using_pandas()
        # self.create_df_using_rdd()
        # self.create_df_using_csv()
        self.create_df_using_log()
        print("")

    def create_df_using_rows(self):
        self.df_using_rows = self.spark.createDataFrame(self._get_list_of_rows())
        self._describe_df(self.df_using_rows, "Dataframe using rows")
        # print("Dataframe using rows:", self.df_using_rows)

    def create_df_using_schema(self):
        self.df_using_schema = self.spark.createDataFrame(
            self._get_list_of_rows(),
            schema="a long, b double, c string, d date, e timestamp"
        )
        self._describe_df(self.df_using_schema, "Dataframe using schema")

    def create_df_using_pandas(self):
        self.df_using_pandas = self.spark.createDataFrame(self._get_pandas_df())
        self._describe_df(self.df_using_pandas, "Dataframe using pandas")

    def create_df_using_rdd(self):
        rdd = self.spark.sparkContext.parallelize(self._get_list_of_tuples())
        self.df_using_rdd = self.spark.createDataFrame(rdd, schema=["a", "b", "c", "d", "e"])
        self._describe_df(self.df_using_rdd, "Dataframe using RDD")

    def create_df_using_csv(self):
        path = self.input_files_dir / "sample.log"

        schema = StructType([
            StructField("timestamp", IntegerType(), False),
            StructField("from_host", StringType(), False),
            StructField("to_host", StringType(), False)
        ])

        self.df_using_csv = self.spark.read.csv(path.as_posix(), header=False, sep=" ", schema=schema)

        with_columns = {
            "from_host": trim("from_host"),
            "to_host": trim("to_host"),
            "timestamp": from_unixtime("timestamp"),
        }
        self.df_using_csv = self.df_using_csv.withColumns(with_columns)
        self._describe_df(self.df_using_csv, "Dataframe using csv")

    def create_df_using_log(self):
        path = self.input_files_dir / "sample.log"

        host_regex = r"(\S+)"
        unix_time_regex = r"(\d+)"

        unix_time_udf = udf(lambda line: re.findall(unix_time_regex, line)[0])
        from_host_udf = udf(lambda line: re.findall(host_regex, line)[1])
        to_host_udf = udf(lambda line: re.findall(host_regex, line)[2])

        self.df_using_log = self.spark.read.text(path.as_posix())
        self.df_using_log = self.df_using_log.select(
            unix_time_udf("value").cast("integer").alias("unix_time"),
            from_host_udf("value").alias("from_host"),
            to_host_udf("value").alias("to_host"),
        )

        self._describe_df(self.df_using_log, "Dataframe using log")

        # List of hostnames connected to a given (configurable) host during the last hour

        # A list of hostnames received connections from a given (configurable) host during the last hour the hostname
        # that generated most connections in the last hour

        # The number of loglines and hostnames can be very high. Consider implementing a CPU and memory-efficient
        # solution. Please feel free to make assumptions as necessary with proper documentation.


    def _setup(self):
        # suppress warnings about future deprecations
        warnings.simplefilter(action='ignore', category=FutureWarning)

    def _describe_df(self, df, title="Dataframe"):
        print("")
        print(f"## {title}")
        print("")
        df.show()
        df.printSchema()
        print(f"Count: {df.count()}")
        print("")
        print(f"Columns: {df.columns}")
        print("")
        print(f"Describe")
        df.describe().show()
        print("")

    def _get_list_of_rows(self):
        return [
            Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(200, 1, 1, 12, 0)),
            Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(200, 1, 2, 12, 0)),
            Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(200, 1, 3, 12, 0)),
        ]

    def _get_list_of_tuples(self):
        return [
            (1, 2., 'string1', date(2000, 1, 1), datetime(200, 1, 1, 12, 0)),
            (2, 3., 'string2', date(2000, 2, 1), datetime(200, 1, 2, 12, 0)),
            (4, 5., 'string3', date(2000, 3, 1), datetime(200, 1, 3, 12, 0)),
        ]

    def _get_pandas_df(self):
        return pd.DataFrame({
            "a": [1, 2, 3],
            "b": [2., 3., 4.],
            "c": ["string1", "string2", "string3"],
            "d": [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
            "e": [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
        })
