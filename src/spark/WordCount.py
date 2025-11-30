import time
from pathlib import Path
from typing import Literal
from multiprocessing import Process, Queue
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import psutil
import os
from functools import wraps
from signal import SIGTERM
import time
import statistics
import resource


def measure_performance(func, sample_interval_secs:int, num_tests:int, *args, **kwargs):
    test_cpu_percents = []
    test_mem_used = []
    test_time = []
    for i in range(num_tests):
        # Set up func to be run as its own process, so we
        # can measure it as it goes. Note that we also set
        # up a psutil process for measurements.
        p = Process(target=func,
                    args=args,
                    kwargs=kwargs)
        p.daemon = False
        psutil_p = psutil.Process(p.pid)
        # Take some starting measurements
        cpu_percent_before = psutil_p.cpu_percent(interval=None)
        mem_used_before = psutil_p.memory_info().rss
        start_time = time.time()
        # Counters so we can calculate the averages
        sum_cpu_percent_measurements = 0
        sum_mem_measurements = 0
        num_measures = 0
        # Start process
        p.start()
        time.sleep(1)
        for child in psutil_p.children():
            # Initial cpu_percent will be 0 for all children
            child.cpu_percent(interval=None)
        while p.is_alive():
            time.sleep(sample_interval_secs)
            sum_cpu_percent_measurements += psutil_p.cpu_percent(interval=None)
            sum_mem_measurements += psutil_p.memory_info().rss
            for child in psutil_p.children():
                sum_cpu_percent_measurements += child.cpu_percent(interval=None)
                sum_mem_measurements += child.memory_info().rss
            num_measures += 1
        # Save performance metrics from this iter.
        end_time = time.time()
        total_time = end_time - start_time
        test_cpu_percents.append(sum_cpu_percent_measurements / num_measures)
        test_mem_used.append(sum_mem_measurements / num_measures)
        test_time.append(total_time)
    # Calculate averages across tests
    avg_cpu_percent = sum(test_cpu_percents) / num_tests
    avg_mem_used = sum(test_mem_used) / num_tests
    avg_time = sum(test_time) / num_tests
    print(f"CPU Usage: {avg_cpu_percent: .2f} %")
    print(f"Memory Usage: {avg_mem_used / (1024**2): .2f} MB")
    print(f"Time: {avg_time: .2f} seconds")
    return


def spark_count_words(spark_session, txt_file:str) -> None:
    """
    Read text in to dataframe,
    convert each line to list of strings and explode.
    Count rows.
    """
    df = spark_session.read.text( f"dataset/{txt_file}")
    df = df.withColumn("word_lists", sf.split(df.value, ' '))
    num_words = df.select( sf.explode(df.word_lists) ).count()
    return num_words


def main(data_set:Literal["toy", "small", "medium"], max_num_cores:int, num_tests:int) -> None:
    """
    Iterate through each number of cores, and run num_tests for each. Print average runtime.
    """
    for i in range(2, max_num_cores+1):
        # Initialize spark session here to avoid init time being counted in runtime.
        spark = SparkSession.builder \
            .appName("WordCount") \
            .master(f"local[{i}]") \
            .config("spark.driver.memory", "15g") \
            .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.13:0.27") \
            .getOrCreate()
        
        #spark_count_words(spark_session=spark, txt_file=data_set)
        print(f"{i} Cores:")
        measure_performance(func=spark_count_words, sample_interval_secs=10,
                            num_tests=2,
                            spark_session=spark, txt_file=data_set)
        spark.stop()


if __name__ == "__main__":
    main(data_set="medium", max_num_cores=4, num_tests=1)
        
        
