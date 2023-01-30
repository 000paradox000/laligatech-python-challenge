from pathlib import Path
import re
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


class Handler:
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent.parent
        self.files_dir = self.base_dir / "files"
        self.input_files_dir = self.files_dir / "input"
        self._setup()
        self.df = None

        self.spark = SparkSession.builder.appName("ll").getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def go(self):
        stream_dir = self.input_files_dir
        lines = self.spark.readStream.text(stream_dir.as_posix())

        host_regex = r"(\S+)"
        from_host_udf = udf(lambda line: re.findall(host_regex, line)[1])
        to_host_udf = udf(lambda line: re.findall(host_regex, line)[2])

        lines = lines.select(
            from_host_udf("value").alias("from_host"),
            to_host_udf("value").alias("to_host"),
        )

        lines = lines.groupBy("from_host", "to_host").count()

        query = lines\
            .writeStream\
            .outputMode("complete")\
            .trigger(processingTime='3600 seconds')\
            .format("console")\
            .start()
        query.awaitTermination()

    def _setup(self):
        # suppress warnings about future deprecations
        warnings.simplefilter(action='ignore', category=FutureWarning)
