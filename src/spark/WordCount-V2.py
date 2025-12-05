"""Spark word-count benchmark with failure-injection and resource tracking.

This module implements a distributed word-count workload using Apache Spark,
with the ability to simulate core failures and measure their impact on performance.
The benchmark tracks CPU, memory, throughput, and recovery overhead across different
core configurations to study fault tolerance and scalability characteristics.
"""

import random  
import time  
import threading  # Enables concurrent resource sampling during Spark job execution
from functools import lru_cache  # Memoizes file/directory size calculations
from pathlib import Path  
from typing import Literal  


# PySpark gives us distributed processing hooks and TaskContext for partition info.
from pyspark import TaskContext  # Provides partition metadata (attempt number, partition ID)
from pyspark.sql import SparkSession  # Entry point for all Spark DataFrame operations
from pyspark.sql import functions as F  # DataFrame helpers for split/explode word counting
import psutil  # Cross-platform library for process and system monitoring



PROJECT_ROOT = Path(__file__).resolve().parents[2]  # Navigate up two levels from this script
DATASET_ROOT = PROJECT_ROOT / "dataset"  # All test datasets live in /dataset subdirectory


def measure_performance(func, sample_interval_secs:int, num_tests:int, *args, **kwargs):
    """Run func num_tests times while sampling resource usage in a background thread.
    
    This wrapper executes the benchmark function multiple times and captures:
    - Average CPU utilization across all driver and executor processes
    - Average memory consumption 
    - Total wall-clock runtime from start to completion
    
    Args:
        func: The benchmark function to execute (e.g., spark_count_words)
        sample_interval_secs: How frequently to poll CPU/memory (in seconds)
        num_tests: Number of independent runs to average over
        *args, **kwargs: Forwarded to the benchmark function
        
    Returns:
        Dictionary with averaged metrics including CPU %, memory MB, runtime,
        throughput, and any function-specific metrics like retry overhead.
    """


    # The sampling thread reads from psutil repeatedly, so cache the Process handle.
    # We reset CPU counters before every run to avoid psutil reporting stale deltas.
    # psutil.cpu_percent() works by comparing snapshots, so each test needs a fresh start.


    # For each test, we'll save metrics to these lists.
    # After all runs complete, we compute averages to reduce noise from transient spikes.
    test_cpu_percents = []  # Average CPU % for each individual run
    test_mem_used = []  # Average memory bytes for each run
    test_time = []  # Total wall-clock seconds for each run
    run_results = []  # Function-specific metrics dict from each run


    for _ in range(num_tests):
        # Each iteration triggers a fresh execution of func plus concurrent sampling.
        # This ensures every test starts with clean CPU/memory baselines.
        
        # Grab the current Python process so we can inspect CPU/memory usage 
        # On local[N] mode, Spark spawns executor threads within this process, so
        # we sum driver + all children to get total resource consumption.
        psutil_proc = psutil.Process()
        
        # psutil needs a warm-up call, otherwise cpu_percent returns 0 on the first read.
        # The interval=None mode uses non-blocking reads after the initial baseline.
        psutil_proc.cpu_percent(interval=None)  # Prime the driver CPU counter
        
        # Also reset CPU counters for any existing child processes (Spark executors)
        for child in psutil_proc.children(recursive=True):
            child.cpu_percent(interval=None)  # Prime each child's CPU counter


        # Buckets that the sampler thread will populate as the job runs.
        # These capture resource snapshots at regular intervals.
        cpu_samples = []  # List of total CPU % readings (driver + all children)
        mem_samples = []  # List of total RSS bytes (driver + all children)
        stop_event = threading.Event()  # Signals the sampler to terminate


        def sample_metrics():
            """Periodically collect CPU and memory usage for the driver and children.
            
            This runs in a background thread, polling psutil at sample_interval_secs
            frequency. We aggregate driver + all children because Spark executors
            run as child processes in local mode.
            """
            # Keep sampling until the caller signals completion via stop_event.
            while not stop_event.is_set():
                # Sum the driver and every child worker so totals reflect Spark executors.
                # cpu_percent() returns percentage relative to a single core, so on
                # an 8-core machine, 800% means all cores saturated.
                cpu_total = psutil_proc.cpu_percent(interval=None)
                mem_total = psutil_proc.memory_info().rss  # Resident Set Size in bytes
                
                # Recursively sum all descendants (Spark may spawn nested processes)
                for child in psutil_proc.children(recursive=True):
                    cpu_total += child.cpu_percent(interval=None)
                    mem_total += child.memory_info().rss
                    
                cpu_samples.append(cpu_total)
                mem_samples.append(mem_total)
                
                # The wait doubles as the sleep interval and stop-event check.
                # Returns True if stop_event was set, False if timeout expired.
                if stop_event.wait(sample_interval_secs):
                    break  # Job finished, exit the sampling loop


        # Launch the sampler alongside the workload and stop it as soon as the run finishes.
        sampler_thread = threading.Thread(target=sample_metrics, daemon=True)
        start_time = time.time()  # Wall-clock start
        sampler_thread.start()  # Begin background resource monitoring
        
        # Execute the actual benchmark (this blocks until Spark job completes)
        run_summary = func(*args, **kwargs)  # Returns dict with job-specific metrics
        
        stop_event.set()  # Signal sampler to stop
        sampler_thread.join()  # Wait for sampler to finish its last iteration
        end_time = time.time()  # Wall-clock end
        
        # Save the function's own metrics so we can aggregate across test runs.
        # Each run_summary includes things like word_count, retry_overhead, etc.
        run_results.append(run_summary)


        # If the job finished before we grabbed a sample, take one final reading.
        # This can happen if the job completes faster than sample_interval_secs.
        if not cpu_samples:
            cpu_samples.append(psutil_proc.cpu_percent(interval=None))
            mem_samples.append(psutil_proc.memory_info().rss)


        # Convert raw sample lists into average readings for this individual run.
        # Averaging smooths out transient spikes and gives a representative load metric.
        total_time = end_time - start_time
        avg_cpu = sum(cpu_samples) / len(cpu_samples)  # Mean CPU % during the run
        avg_mem = sum(mem_samples) / len(mem_samples)  # Mean memory bytes during the run
        
        # Append this run's averages to the cross-run lists
        test_cpu_percents.append(avg_cpu)
        test_mem_used.append(avg_mem)
        test_time.append(total_time)
        
    # Calculate averages across tests and report derived metrics.
    # Multiple runs reduce variance from JIT warmup, GC pauses, etc.
    avg_cpu_percent = sum(test_cpu_percents) / num_tests
    avg_mem_used = sum(test_mem_used) / num_tests
    avg_time = sum(test_time) / num_tests


    # Aggregate run-specific stats so callers receive a single averaged summary.
    # initialization_overhead: time spent building the DataFrame DAG before any action
    # processing_time: time spent executing the Spark action (count)
    # retry_overhead: time spent reprocessing failed partitions
    avg_init_overhead = sum(r["initialization_overhead"] for r in run_results) / num_tests
    avg_processing_time = sum(r["processing_time"] for r in run_results) / num_tests
    avg_retry_overhead = sum(r.get("retry_overhead", 0.0) for r in run_results) / num_tests
    
    # These dataset-level values do not change between runs, so reuse the first result.
    # No point averaging the word count since it's deterministic per dataset.
    dataset_bytes = run_results[0]["dataset_bytes"] if run_results else 0
    word_count = run_results[0]["word_count"] if run_results else 0
    failure_reschedules = run_results[0].get("failure_reschedules", 0) if run_results else 0


    # Translate dataset size and processing time into throughput-style metrics.
    # Throughput helps compare performance across different datasets and core counts.
    throughput_words = word_count / avg_processing_time if avg_processing_time else 0  # words/sec
    throughput_mb = (dataset_bytes / (1024 ** 2)) / avg_processing_time if avg_processing_time else 0  # MB/sec


    # Bundle all values into a single dict so callers can emit or compare them easily.
    return {
        "cpu_percent": avg_cpu_percent,  # Average CPU % across all samples and runs
        "memory_mb": avg_mem_used / (1024 ** 2),  # Average memory in MB
        "runtime": avg_time,  # Average wall-clock seconds
        "initialization_overhead": avg_init_overhead,  # Time to build DataFrame DAG
        "processing_time": avg_processing_time,  # Time executing Spark action
        "throughput_words": throughput_words,  # Words processed per second
        "throughput_mb": throughput_mb,  # Megabytes processed per second
        "word_count": word_count,  # Total words in dataset (deterministic)
        "dataset_bytes": dataset_bytes,  # Raw byte count
        "failure_reschedules": failure_reschedules,  # How many partitions were retried
        "failed_core_budget": run_results[0].get("failed_core_budget", 0) if run_results else 0,  # Injected failure count
        "total_cores": run_results[0].get("total_cores") if run_results else None,  # Parallelism level
        "retry_overhead": avg_retry_overhead,  # Time spent on manual recovery
    }



@lru_cache(maxsize=16)
def _path_size_bytes(target_path: Path) -> int:
    """Return total bytes for a file or directory (recursively).
    
    Cached to avoid re-scanning the same dataset multiple times.
    Useful for calculating throughput (bytes/sec) and disk usage metrics.
    
    Args:
        target_path: File or directory to measure
        
    Returns:
        Total size in bytes (sum of all files if directory)
    """
    if target_path.is_file():
        # Base case: single file, so its stat size is the answer.
        return target_path.stat().st_size
        
    # Recursive case: sum all files within the directory tree
    total = 0
    for file_path in target_path.rglob("*"):  # rglob("*") recursively walks all descendants
        if file_path.is_file():
            # Add every nested file so directory sizes reflect on-disk footprint.
            total += file_path.stat().st_size
    return total



def _load_dataset_lines_df(spark_session:SparkSession, txt_file:str):
    """Return the dataset path metadata plus a DataFrame of text lines.
    
    Centralizes dataset loading so both baseline and failure modes share identical
    ingestion logic. Handles both single files and directories of text files and
    returns the raw DataFrame so callers can build word lists via split/explode.
    
    Args:
        spark_session: Active Spark session
        txt_file: Relative path from DATASET_ROOT (e.g., "toy", "small", "medium")
        
    Returns:
        Tuple of (resolved_path, lines_df, total_bytes)
        
    Raises:
        FileNotFoundError: If the dataset path doesn't exist
    """
    # Construct absolute path relative to the project's dataset directory
    dataset_path = (DATASET_ROOT / txt_file).resolve()
    
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset path not found: {dataset_path}")
        
    # Spark's DataFrame reader handles both files and directories under the dataset path.
    # read.text() treats each line as a single-column DataFrame with column name "value"

    lines_df = spark_session.read.text(str(dataset_path))
    
    # Calculate total dataset size for throughput metrics
    dataset_bytes = _path_size_bytes(dataset_path)
    
    return dataset_path, lines_df, dataset_bytes



def _fail_partitions_once(rdd, failing_partitions:set[int]):
    """Force the first attempt of specified partitions to fail to mimic core loss.
    
    This injects transient failures that Spark should recover from via task retry.
    Only the first attempt fails; subsequent retries succeed, simulating a core
    that temporarily goes offline and then recovers.
    
    Args:
        rdd: Input RDD to wrap with failure injection
        failing_partitions: Set of partition indices that should fail on first attempt
        
    Returns:
        Transformed RDD that raises exceptions for specified partitions on attempt 0
    """


    def maybe_fail(partition_index:int, iterator):
        """Conditionally raise an exception based on partition and attempt number.
        
        Args:
            partition_index: Which partition this is (0-indexed)
            iterator: The partition's data iterator
            
        Returns:
            The original iterator if we're not failing this partition
            
        Raises:
            Exception: On first attempt for partitions in failing_partitions set
        """
        # TaskContext is only populated on executors, so guard access in case.
        # In local mode it's available; in some edge cases it might be None.
        ctx = TaskContext.get()
        
        # Check three conditions: context exists, this partition should fail, first attempt
        if ctx and partition_index in failing_partitions and ctx.attemptNumber() == 0:
            # Only the first attempt should fail so Spark retries and then continues.
            # attemptNumber() starts at 0, increments on each retry.
            raise Exception(f"Simulated core failure on partition {partition_index}")
            
        # Either we're not failing this partition, or this is a retry (attempt >= 1)
        return iterator


    # mapPartitionsWithIndex applies maybe_fail to each partition, passing the index
    return rdd.mapPartitionsWithIndex(maybe_fail)



def spark_count_words(spark_session, txt_file:str, total_cores:int) -> dict:
    """Baseline execution: count words with no simulated core failures.
    
    This establishes the steady-state performance profile for comparison against
    failure scenarios. Measures initialization overhead (DataFrame construction)
    and processing time (action execution) separately for granular analysis.
    
    Args:
        spark_session: Active Spark session configured with desired parallelism
        txt_file: Dataset name (relative to DATASET_ROOT)
        total_cores: Number of cores configured for this session (for metadata)
        
    Returns:
        Dictionary with word_count, timing breakdowns, and zero failure metrics
    """


    job_start = time.time()  # Mark the overall job start
    
    # Reuse the shared loader so both baseline and failure modes ingest data identically.
    # This ensures apples-to-apples comparison between runs.
    _, lines_df, dataset_bytes = _load_dataset_lines_df(spark_session, txt_file)

    action_start = time.time()  # Mark when we begin the actual Spark action

    # Build a DataFrame of individual words using split + explode so we mirror
    words_df = (
        lines_df
        .select(
            F.explode(
                F.split(
                    F.coalesce(F.col("value"), F.lit("")),
                    "\\s+",
                )
            ).alias("word")
        )
        .filter(F.col("word") != "")
    )

    # Counting rows on the exploded DataFrame yields the dataset's word count.
    num_words = words_df.count()
    
    end_time = time.time()  # Job complete


    # Return detailed metrics so the caller can analyze performance breakdown
    return {
        "word_count": num_words,  # Total words processed
        "dataset_bytes": dataset_bytes,  # Dataset size for throughput calculation
        "initialization_overhead": action_start - job_start,  # Time spent building DataFrame DAG
        "processing_time": end_time - action_start,  # Time executing the distributed count
        "failure_reschedules": 0,  # No failures in baseline mode
        "failed_core_budget": 0,  # No injected failures
        "total_cores": total_cores,  # Parallelism level (for metadata)
        "retry_overhead": 0.0,  # No retries needed
    }



def spark_count_words_with_core_failures(spark_session, txt_file:str, failed_cores:int, total_cores:int) -> dict:
    """Simulate losing failed_cores by forcing that many partitions to restart once.
    
    This mimics a realistic failure scenario where some executors crash mid-job.
    Spark's fault tolerance should automatically retry the failed partitions.
    We measure both automatic retry overhead (Spark-level) and manual recovery
    overhead (if Spark gives up and we rebuild the dataset end-to-end).
    
    Args:
        spark_session: Active Spark session
        txt_file: Dataset name
        failed_cores: How many partitions to fail (proxy for lost cores)
        total_cores: Total parallelism level
        
    Returns:
        Dictionary with word_count, timing, retry overhead, and failure metadata
    """


    job_start = time.time()
    _, lines_df, dataset_bytes = _load_dataset_lines_df(spark_session, txt_file)
    action_start = time.time()

    # Build the exploded DataFrame of words, then convert to an RDD so we can
    # reuse the partition-failure helper (which operates on RDD partitions).
    words_df = (
        lines_df
        .select(
            F.explode(
                F.split(
                    F.coalesce(F.col("value"), F.lit("")),
                    "\\s+",
                )
            ).alias("word")
        )
        .filter(F.col("word") != "")
    )
    words_rdd = words_df.rdd.map(lambda row: row.word)

    # Determine how many partitions to fail (can't fail more than total partitions)
    total_partitions = words_rdd.getNumPartitions()
    
    # Never try to fail more partitions than we actually have.
    failing_count = min(failed_cores, total_partitions)
    
    # Randomly select which partitions will fail to simulate unpredictable core loss
    failing_partitions = set(random.sample(range(total_partitions), failing_count)) if failing_count else set()
    
    retry_overhead = 0.0  # Track time spent on manual recovery if Spark bails out
    
    if failing_partitions:
        # Wrap the RDD so the chosen partitions raise exactly once on their first attempt.
        # This transforms the words_rdd into a failure-injected version.
        words_rdd = _fail_partitions_once(words_rdd, failing_partitions)
        


    try:
        # In the common case Spark retries the failing partitions internally and count still works.
        # Spark's task scheduler detects the failure and automatically resubmits those tasks.
        num_words = words_rdd.count()
        end_time = time.time()
        
    except Exception as err:
        # If Spark exhausted retries or we hit a non-simulated error, handle it
        if not failing_partitions or "Simulated core failure" not in str(err):
            raise  # Unexpected error, propagate it
            
        # Spark bailed out because of our injected failure. Rebuild the dataset and measure retry time.
        # This simulates a scenario where the job fails and we manually restart it.
        recover_start = time.time()
        
        # Load a fresh DataFrame without failure injection and rebuild the word DataFrame.
        _, retry_df, _ = _load_dataset_lines_df(spark_session, txt_file)
        retry_words_df = (
            retry_df
            .select(
                F.explode(
                    F.split(
                        F.coalesce(F.col("value"), F.lit("")),
                        "\\s+",
                    )
                ).alias("word")
            )
            .filter(F.col("word") != "")
        )
        
        # Re-run the word count on the clean DataFrame (no simulated failures).
        num_words = retry_words_df.count()

        end_time = time.time()
        
        # Calculate how much extra time the manual recovery added
        retry_overhead = end_time - recover_start


    # Return comprehensive metrics including failure-specific data
    return {
        "word_count": num_words,
        "dataset_bytes": dataset_bytes,
        "initialization_overhead": action_start - job_start,
        "processing_time": end_time - action_start,  # Includes automatic retry time
        "failure_reschedules": len(failing_partitions),  # How many partitions we failed
        "failed_core_budget": failed_cores,  # Requested failure count
        "total_cores": total_cores,
        "retry_overhead": retry_overhead,  # Manual recovery time (usually 0 if Spark handles it)
    }



def main(data_set:Literal["toy", "small", "medium"],
         max_num_cores:int, num_tests:int,
         mem_frac:float=0.6, num_partitions:int=200,
         core_failure_policy:dict={4: 0, 3: 0, 2: 0, 1: 0}) -> None:
    """Compare steady-state vs failure runs for each core count and print metrics.
    
    1. For each core count from 1 to max_num_cores:
       a. Run baseline word-count (no failures)
       b. Run failure-injected word-count (if policy dictates failures at this core count)
    2. Calculate speedup relative to single-core baseline
    3. Print detailed metrics for each configuration
    4. Print summary scalability and failure-impact tables
    
    Args:
        data_set: Which dataset to use ("toy", "small", or "medium")
        max_num_cores: Maximum parallelism level to test (tests 1..max_num_cores)
        num_tests: How many runs to average for each configuration
        mem_frac: Fraction of JVM heap space (300 MiB, ~314 MB) used for
            spark execution and storage.
        num_partitions: Number of partitions to use when shuffling data.
        core_failure_policy: Dict. Define how many simulated cores we "lose"
            at each tested parallelism level. Unlisted core counts have 0 failures.
    """
    
    baseline_runtime = None  # Will store single-core runtime for speedup calculation
    per_core_metrics = []  # List of (cores, baseline_metrics, failure_metrics, scalability)


    # Test each core count from 1 to max_num_cores inclusive
    for i in range(1, max_num_cores+1):
        # Bring up a fresh SparkSession so resource usage per core count stays isolated.
        # Stopping and restarting Spark ensures no state leaks between configurations.
        spark = SparkSession.builder \
            .appName("WordCount") \
            .master(f"local[{i}]") \
            .config("spark.driver.memory", "15g") \
            .config("spark.task.maxFailures", "100") \
            .config("spark.memory.fraction", f"{mem_frac: .2f}") \
            .config("spark.sql.shuffle.partitions", f"{num_partitions}") \
            .getOrCreate()

        # Measure steady-state performance for this core count.
        # This gives us the baseline to compare against failure scenarios.
        baseline_metrics = measure_performance(
            func=spark_count_words,  # Baseline function (no failures)
            sample_interval_secs=5,  # Poll CPU/memory every 5 seconds
            num_tests=num_tests,  # Average over multiple runs
            spark_session=spark,
            txt_file=data_set,
            total_cores=i,
        )


        # Check if this core count has a defined failure budget in our policy
        failure_budget = core_failure_policy.get(i, 0)
        failure_metrics = None
        
        if failure_budget > 0:
            # Re-run with injected failures to study how recovery affects the totals.
            # Same dataset, same core count, but with simulated partition failures.
            failure_metrics = measure_performance(
                func=spark_count_words_with_core_failures,  # Failure-injection function
                sample_interval_secs=5,
                num_tests=num_tests,
                spark_session=spark,
                txt_file=data_set,
                failed_cores=failure_budget,  # How many partitions to fail
                total_cores=i,
            )


        # Clean shutdown of Spark to free resources before starting next configuration
        spark.stop()


        if baseline_runtime is None:
            # Cache the one-core runtime so we can compute speedup for every other core.
            # Speedup = T(1 core) / T(N cores), where higher is better.
            baseline_runtime = baseline_metrics["runtime"]


        # Store every result so we can print both per-core and cross-core summaries later.
        scalability = (baseline_runtime / baseline_metrics["runtime"]) if baseline_metrics["runtime"] else 0
        per_core_metrics.append((i, baseline_metrics, failure_metrics, scalability))


        # We'll save our metrics to the log file.
        with open("log.txt", "a") as log:
            # Print baseline (no-failure) metrics first
            print("\nInitialization Overhead (no failures): "
                f"{baseline_metrics['initialization_overhead']: .2f} seconds",
                file=log)
            print(f"{i} Cores (steady state):", file=log)
            print(f"CPU Usage: {baseline_metrics['cpu_percent']: .2f} %", file=log)
            print(f"Memory Usage: {baseline_metrics['memory_mb']: .2f} MB", file=log)
            print(f"RunTime: {baseline_metrics['runtime']: .2f} seconds", file=log)
            print("**Throughput:** "
                f"{baseline_metrics['throughput_words']: .2f} words/sec |"
                f" {baseline_metrics['throughput_mb']: .2f} MB/sec", file=log)
            print("**Scalability:** "
                f"{scalability: .2f}x relative to 1 core", file=log)


            if failure_metrics:
                # Compare steady-state runtime with the failure run to quantify the penalty.
                # Recovery overhead = total failure runtime - baseline runtime
                recovery_overhead = failure_metrics["runtime"] - baseline_metrics["runtime"]
                
                print("\nInitialization Overhead (simulated core failure): "
                    f"{failure_metrics['initialization_overhead']: .2f} seconds", file=log)
                print(f"{i} Cores with {failure_budget} core failure(s):", file=log)
                print(f"CPU Usage: {failure_metrics['cpu_percent']: .2f} %", file=log)
                print(f"Memory Usage: {failure_metrics['memory_mb']: .2f} MB", file=log)
                print(f"RunTime: {failure_metrics['runtime']: .2f} seconds", file=log)
                print("**Throughput:** "
                    f"{failure_metrics['throughput_words']: .2f} words/sec |"
                    f" {failure_metrics['throughput_mb']: .2f} MB/sec", file=log)
                
                # Side-by-side comparison of baseline vs failure runtime
                print(f"Total Execution Time (no failures): {baseline_metrics['runtime']: .2f} seconds", file=log)
                print(f"Total Execution Time (with failures): {failure_metrics['runtime']: .2f} seconds", file=log)
                print(f"Recovery Overhead: {recovery_overhead: .2f} seconds", file=log)
                print(f"Measured Retry Overhead (time spent reprocessing): {failure_metrics['retry_overhead']: .2f} seconds", file=log)
                
                if failure_metrics["failure_reschedules"]:
                    # Spark reports how many partitions had to be retried, which approximates lost cores.
                    # This confirms our failure injection actually triggered retries.
                    print("Rescheduled partitions (proxy for failed cores): "
                        f"{failure_metrics['failure_reschedules']}", file=log)


            # Print summary scalability table showing speedup at each core count
            print("\n**Scalability:** T₁ / T across core configurations", file=log)
            for core_count, _, _, scala in per_core_metrics:
                # Ideal linear speedup would be 1x, 2x, 3x, 4x for 1, 2, 3, 4 cores.
                print(f"{core_count} cores: {scala: .2f}x", file=log)




if __name__ == "__main__":
    # Clear the log file each run
    with open("log.txt", 'w') as file:
        pass
    main(data_set="small", max_num_cores=8, num_tests=1, mem_frac=( 128/(15000-300) ), num_partitions=200,
         core_failure_policy={8: 0, 4: 0, 2: 0, 1: 0})

