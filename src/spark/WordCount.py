import time
from pathlib import Path
from typing import Literal
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

def spark_count_words(spark_session, txt_file:str, num_cores:int) -> None:
    """
    Read text in to dataframe,
    convert each line to list of strings and explode.
    Count rows.
    """
    df = spark_session.read.text(txt_file)
    df = df.withColumn("word_lists", sf.split(df.value, ' '))
    num_words = df.select( sf.explode(df.word_lists) ).count()


def time_spark(data_set:Literal["toy", "small", "medium"], max_num_cores:int, num_tests:int) -> None:
    """
    Iterate through each number of cores, and run num_tests for each. Print average runtime.
    """
    for i in range(1, max_num_cores+1):
        # Initialize spark session here to avoid init time being counted in runtime.
        spark = SparkSession.builder \
            .appName("WordCount") \
            .master(f"local[{i}]") \
            .config("spark.driver.memory", "15g") \
            .getOrCreate()
        total_time = 0
        for test_num in range(num_tests):
            # Start time, run word count, and check elapsed time.
            start_time = time.perf_counter()
            spark_count_words(spark_session=spark, txt_file=data_set, num_cores=i)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
        # Report avg runtime and then stop this session of spark.
        print(f"{i} Cores: {total_time / num_tests:.6f} seconds")
        spark.stop()


if __name__ == "__main__":
    time_spark(data_set="medium", max_num_cores=4, num_tests=1)
        
        
