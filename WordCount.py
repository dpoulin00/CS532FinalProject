from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

def spark_count_words(txt_file:str) -> None:
    spark = SparkSession.builder.appName("WordCount").getOrCreate()
    df = spark.read.text(txt_file).cache()
    df = df.repartition(100)
    df = df.withColumn("word_lists", sf.split(df.value, ' '))
    num_words = df.select( sf.explode(df.word_lists) ).count()
    print("Number of Words: %i" % (num_words))
    spark.stop()


if __name__ == "__main__":
    text_file = "hamlet.txt"
    spark_count_words("hamlet.txt")
