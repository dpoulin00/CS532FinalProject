import subprocess
import time
import psutil
import csv
import threading
import os
import signal
import json
import urllib.request
from tqdm import tqdm

HADOOP_JAR = "WordCount.jar"
CLASS_NAME = "WordCount"
INPUT_ROOT = "/user/rishab/input" 
OUTPUT_ROOT = "/user/rishab/output"
RESULTS_FILE = "hadoop_ft_results.csv"
YARN_URL = "http://localhost:8088/ws/v1/cluster/apps"

monitoring = False
metrics = []
events = []

def monitor_system(interval=1):
    """Records CPU/RAM/Disk in background."""
    global monitoring, metrics, events
    while monitoring:
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_io_counters()
        
        current_event = events.pop(0) if events else None

        metrics.append({
            "timestamp": time.time(),
            "cpu": cpu,
            "memory": mem,
            "disk_read": disk.read_bytes,
            "disk_write": disk.write_bytes,
            "event": current_event
        })
        time.sleep(interval)

def get_job_progress():
    """Ask YARN how far along the job is."""
    try:
        with urllib.request.urlopen(f"{YARN_URL}?state=RUNNING") as response:
            data = json.loads(response.read().decode())
            if data['apps'] and 'app' in data['apps']:
                return float(data['apps']['app'][0].get('progress', 0))
    except:
        pass
    return 0.0

def restart_yarn_with_cores(num_cores):
    """Restricts YARN to specific cores using taskset."""
    hadoop_home = os.environ.get('HADOOP_HOME')
    cpu_list = f"0-{num_cores-1}"
    
    # Stop
    subprocess.run(f"{hadoop_home}/bin/yarn --daemon stop nodemanager", 
                   shell=True, stderr=subprocess.DEVNULL)
    time.sleep(3)
    
    # Start with CPU core restriction
    subprocess.run(f"taskset -c {cpu_list} {hadoop_home}/bin/yarn --daemon start nodemanager", shell=True)
    
    # Wait for registration
    time.sleep(10)

def kill_one_worker_task():
    """Finds a YarnChild (Task) and kills it."""
    try:
        cmd = ["jps"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()
        
        # Look for the LAST YarnChild (most recently started)
        pids = []
        for line in stdout.decode().splitlines():
            if "YarnChild" in line:
                pids.append(int(line.split()[0]))
        
        if pids:
            target = pids[-1]
            print(f"\n[CHAOS] Killing Task PID {target}...")
            os.kill(target, signal.SIGKILL)
            return True
    except Exception as e:
        print(f"Kill failed: {e}")
    return False

def run_experiment(dataset_name, num_cores, sort_mb):
    global monitoring, metrics, events
    
    restart_yarn_with_cores(num_cores)
    
    monitoring = True
    metrics = []
    events = []
    
    input_path = f"{INPUT_ROOT}/{dataset_name}"
    output_path = f"{OUTPUT_ROOT}/{dataset_name}_{num_cores}c_FT"
    subprocess.run(f"hdfs dfs -rm -r -f {output_path}", shell=True, stderr=subprocess.DEVNULL)

    print(f"\n--- TEST: {dataset_name} | Cores: {num_cores} | Fault Injection: ON ---")

    cmd = (
        f"hadoop jar {HADOOP_JAR} {CLASS_NAME} "
        f"-D mapreduce.job.maps={num_cores} "
        f"-D mapreduce.job.reduces={num_cores} "
        f"-D mapreduce.map.maxattempts=4 " 
        f"-D mapreduce.reduce.maxattempts=4 "
        f"-D mapreduce.task.io.sort.mb={sort_mb} "
        f"{input_path} {output_path}"
    )

    t = threading.Thread(target=monitor_system)
    t.start()
    
    start_time = time.time()

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    failure_triggered = False
    
    while process.poll() is None:
        if not failure_triggered:
            progress = get_job_progress()
            print(f"\rProgress: {progress:.1f}%", end="")
            
            # TRIGGER AT 50%
            if progress >= 50.0:
                print("\n!!! 50% Reached. Injecting Failure... !!!")
                if kill_one_worker_task():
                    events.append("TASK_KILLED")
                    failure_triggered = True
                    print("Task killed. Waiting for Hadoop to auto-recover...")
                else:
                    print("Could not find task to kill.")
                    failure_triggered = True # Stop trying
        
        time.sleep(1)

    # 7. Cleanup
    end_time = time.time()
    monitoring = False
    t.join()
    
    duration = end_time - start_time
    
    # Check if it actually succeeded
    if process.returncode == 0:
        print(f"\nSUCCESS. Job recovered. Total Time: {duration:.2f}s")
        outcome = "Recovered"
    else:
        print(f"\nFAILED. Return Code: {process.returncode}")
        outcome = "Failed"

    return {
        "dataset": dataset_name,
        "cores": num_cores,
        "runtime": duration,
        "outcome": outcome
    }

def main():
    try:
        datasets = ["toy"] 
        core_counts = [1, 2, 4, 8]
        
        # Init CSV
        with open(RESULTS_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['dataset', 'cores', 'runtime', 'outcome'])
            writer.writeheader()

        for d in datasets:
            for c in tqdm(core_counts):
                res = run_experiment(d, c, 100)
                
                with open(RESULTS_FILE, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=['dataset', 'cores', 'runtime', 'outcome'])
                    writer.writerow(res)
                    
                metrics_filename = f"ft_metrics_{d}_{c}cores.csv"
                with open(metrics_filename, 'w', newline='') as f:
                    fieldnames = ['timestamp', 'cpu', 'memory', 'disk_read', 'disk_write', 'event']
                    
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(metrics)

    finally:
        # Reset NodeManager to normal
        subprocess.run(f"{os.environ.get('HADOOP_HOME')}/bin/yarn --daemon stop nodemanager", shell=True)

if __name__ == "__main__":
    main()