import subprocess
import time
import psutil
import csv
import threading
import os
from tqdm import tqdm

HADOOP_JAR = "WordCount.jar"
CLASS_NAME = "WordCount"
# HDFS Paths
INPUT_ROOT = "/user/rishab/input" 
OUTPUT_ROOT = "/user/rishab/output"
# CSV Report File
RESULTS_FILE = "hadoop_experiment_results.csv"

monitoring = False
metrics = []

def monitor_system(interval=1):
    global monitoring, metrics
    while monitoring:
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_io_counters()
        
        metrics.append({
            "timestamp": time.time(),
            "cpu": cpu,
            "memory": mem,
            "disk_read": disk.read_bytes,
            "disk_write": disk.write_bytes
        })
        time.sleep(interval)


def upload_dataset_to_hdfs(local_path, hdfs_path):
    """
    Uploads local dataset folder to HDFS.
    local_path: local folder path (e.g., '../../dataset')
    hdfs_path: HDFS destination folder (e.g., '/user/rishab/input')
    """
    subprocess.run(f"hdfs dfs -mkdir -p {hdfs_path}", shell=True, stderr=subprocess.DEVNULL)
    
    for item in os.listdir(local_path):
        local_item_path = os.path.join(local_path, item)
        
        if os.path.isdir(local_item_path):
            hdfs_item_path = os.path.join(hdfs_path, item)
            
            exists = subprocess.run(
                f"hdfs dfs -test -d {hdfs_item_path}",
                shell=True
            ).returncode == 0
            
            if exists:
                print(f"Skipping {local_item_path}: already exists in HDFS")
                continue
            
            print(f"Uploading {local_item_path} to {hdfs_item_path}")
            subprocess.run(f"hdfs dfs -put {local_item_path} {hdfs_item_path}", shell=True)
        else:
            print(f"Skipping {local_item_path} (not a directory)")


def restart_yarn_with_cores(num_cores):
    """
    Restart YARN NodeManager with CPU core restriction
    """
    hadoop_home = os.environ.get('HADOOP_HOME')
    if not hadoop_home:
        raise Exception("HADOOP_HOME environment variable not set!")
    
    cpu_list = f"0-{num_cores-1}"
    
    print(f"Restarting YARN NodeManager restricted to cores {cpu_list}...")
    
    # Stop NodeManager
    subprocess.run(
        f"{hadoop_home}/bin/yarn --daemon stop nodemanager",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    
    # Wait for clean shutdown
    time.sleep(5)
    
    # Check if NodeManager actually stopped
    for _ in range(10):
        result = subprocess.run(
            "pgrep -f 'NodeManager'",
            shell=True,
            capture_output=True
        )
        if result.returncode != 0:  # No process found
            break
        time.sleep(2)
    
    # Start NodeManager with taskset
    subprocess.run(
        f"taskset -c {cpu_list} {hadoop_home}/bin/yarn --daemon start nodemanager",
        shell=True
    )
    
    # Wait for NodeManager to be ready
    print("Waiting for NodeManager to initialize...")
    time.sleep(15)
    
    # Verify NodeManager is running
    result = subprocess.run(
        "pgrep -f 'NodeManager'",
        shell=True,
        capture_output=True
    )
    if result.returncode == 0:
        print(f"NodeManager started successfully on cores {cpu_list}")
    else:
        raise Exception("NodeManager failed to start!")


def verify_core_usage():
    """
    Verify which cores are being used by container processes
    """
    result = subprocess.run(
        "ps -eLo psr,comm | grep 'Container' | awk '{print $1}' | sort -n | uniq",
        shell=True,
        capture_output=True,
        text=True
    )
    if result.stdout:
        cores = result.stdout.strip().split('\n')
        print(f"Container processes running on cores: {', '.join(cores)}")
        return cores
    return []


def run_hadoop_job(dataset_name, num_cores, sort_mb):
    global monitoring, metrics
    
    # Restart NodeManager with core restriction
    restart_yarn_with_cores(num_cores)
    
    monitoring = True
    metrics = []
    
    input_path = f"{INPUT_ROOT}/{dataset_name}"
    output_path = f"{OUTPUT_ROOT}/{dataset_name}_{num_cores}cores_{sort_mb}mb"
    
    subprocess.run(f"hdfs dfs -rm -r -f {output_path}", shell=True, stderr=subprocess.DEVNULL)

    print(f"\nRunning: Dataset={dataset_name}, Cores={num_cores}, SortMB={sort_mb}")

    cmd = (
        f"hadoop jar {HADOOP_JAR} {CLASS_NAME} "
        f"-D mapreduce.job.maps={num_cores} "
        f"-D mapreduce.job.reduces={num_cores} "
        f"-D mapreduce.map.cpu.vcores=1 "
        f"-D mapreduce.reduce.cpu.vcores=1 "
        f"-D yarn.app.mapreduce.am.resource.cpu-vcores=1 "
        f"-D mapreduce.task.io.sort.mb={sort_mb} "
        f"{input_path} {output_path}"
    )

    t = threading.Thread(target=monitor_system)
    t.start()

    start_time = time.time()
    process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    end_time = time.time()

    monitoring = False
    t.join()
    
    # Verify core usage after job completes
    print("\nCore usage verification:")
    verify_core_usage()

    if process.returncode != 0:
        print("ERROR: Hadoop job failed!")
        print(process.stderr.decode())
        return None

    duration = end_time - start_time
    avg_cpu = sum(m['cpu'] for m in metrics) / len(metrics) if metrics else 0
    avg_mem = sum(m['memory'] for m in metrics) / len(metrics) if metrics else 0
    
    print(f"Completed in {duration:.2f} seconds")
    
    return {
        "dataset": dataset_name,
        "cores": num_cores,
        "sort_mb": sort_mb,
        "runtime_seconds": duration,
        "avg_cpu": avg_cpu,
        "avg_mem": avg_mem,
    }


def cleanup_yarn():
    """
    Stop NodeManager at the end of experiments
    """
    hadoop_home = os.environ.get('HADOOP_HOME')
    if hadoop_home:
        print("\nCleaning up: Stopping NodeManager...")
        subprocess.run(
            f"{hadoop_home}/bin/yarn --daemon stop nodemanager",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(3)


def main():
    try:
        # Upload datasets
        upload_dataset_to_hdfs("../../dataset", INPUT_ROOT)

        datasets = ["toy", "small"] 
        core_counts = [1, 2, 4, 8] 
        results = []

        for d in datasets:
            for c in tqdm(core_counts, desc=f"Cores for {d}", leave=False):
                res = run_hadoop_job(d, num_cores=c, sort_mb=100)

                if res is not None:
                    results.append(res)
                    
                    # Save results after each successful run
                    keys = ['dataset', 'cores', 'sort_mb', 'runtime_seconds', 'avg_cpu', 'avg_mem']
                    with open(RESULTS_FILE, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(results)
                    
                    print(f"Results saved to {RESULTS_FILE}")
                else:
                    print(f"Skipping failed job: {d} with {c} cores")
                
                # Small delay between jobs
                time.sleep(2)
        
        print("\n" + "="*60)
        print("All experiments completed!")
        print(f"Results saved in: {RESULTS_FILE}")
        print("="*60)
    
    finally:
        # Always cleanup on exit
        cleanup_yarn()


if __name__ == "__main__":
    main()