# from libs.handler import Handler
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def main():
    # handler = Handler()
    # handler.go()

    spark = SparkSession.builder.appName("streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Create DataFrame representing the stream of input lines from
    # connection to localhost:9999
    lines = spark.readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()

    # Split the lines into words
    words = lines.select(explode(split(lines.value, " ")).alias("word"))

    # Generate running word count
    word_counts = words.groupBy("word").count()

    # Start running the query that prints the running counts to the console
    query = word_counts\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
