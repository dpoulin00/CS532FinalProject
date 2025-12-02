import time
from typing import Literal
from multiprocessing import Process
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st
import psutil


def measure_performance(func, sample_interval_secs:int, num_tests:int, *args, **kwargs):
    """
    func: Function whose performance we'll measure.
    sample_interval_secs: While function runs, number of seconds between measuring performance.
    num_tests: Number of times to run function.
    args, kwargs: passed to func.
    """
    # For each test, we'll save metrics to these lists.
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
        start_time = time.time()
        
        # Counters so we can calculate the averages
        sum_cpu_percent_measurements = 0
        sum_mem_measurements = 0
        num_measures = 0
        
        # Start process
        p.start()
        time.sleep(1)
        
        for child in psutil_p.children():
            # Initial cpu_percent will be 0 for all children, but
            # we must call this now so cpu_percent has a starting point.
            child.cpu_percent(interval=None)
            
        while p.is_alive():
            # Every interval, we measure CPU percentage and memory
            # usage of the main process and any children.
            time.sleep(sample_interval_secs)
            sum_cpu_percent_measurements += psutil_p.cpu_percent(interval=None)
            sum_mem_measurements += psutil_p.memory_info().rss
            for child in psutil_p.children(recursive=True):
                sum_cpu_percent_measurements += child.cpu_percent(interval=None)
                sum_mem_measurements += child.memory_info().rss
            num_measures += 1
            
        # Save performance metrics from this test.
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


def spark_count_words(spark_session, txt_file:str, simulate_task_errors:bool=False) -> None:
    """
    Read text in to dataframe,
    convert each line to list of strings and explode.
    Count rows.
    """
    
    def simulate_failure(value):
        """Applying this function to a string column will simulate task fail with prob 0.2"""
        simulate_fail_flag = np.random.choice(a=[True, False], p=[0.2, 0.8], replace=True)
        if simulate_fail_flag:
            raise Exception("Simulated Fail") from None
        return value
    
    simulate_failure_udf = sf.udf(simulate_failure, st.StringType())
            
    df = spark_session.read.text( f"dataset/{txt_file}")
    
    if simulate_task_errors:
        df = df.withColumn("value", simulate_failure_udf(df["value"]))
        
    df = df.withColumn("word_lists", sf.split(df.value, ' '))
    num_words = df.select( sf.explode(df.word_lists) ).count()
    
    return num_words


def main(data_set:Literal["toy", "small", "medium"], max_num_cores:int, num_tests:int) -> None:
    """
    Iterate through each number of cores, and run num_tests for each. Print average runtime.
    """
    for i in range(1, max_num_cores+1):
        # Initialize spark session here to avoid init time being counted in runtime.
        spark = SparkSession.builder \
            .appName("WordCount") \
            .master(f"local[{i}]") \
            .config("spark.driver.memory", "15g") \
            .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.13:0.27") \
            .config("spark.task.maxFailures", 100) \
            .getOrCreate()
        # Measure performance with different numbers of cores.
        print(f"{i} Cores:")
        measure_performance(func=spark_count_words, sample_interval_secs=5,
                            num_tests=2,
                            spark_session=spark, txt_file=data_set, simulate_task_errors=True)
        spark.stop()


if __name__ == "__main__":
    main(data_set="toy", max_num_cores=4, num_tests=1)