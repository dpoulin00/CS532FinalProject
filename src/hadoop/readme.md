# Hadoop Benchmarking on Word Count

This repository contains code, data and plots used to benchmark MapReduce (Hadoop) 

> Quick run order:
> 1. `./compile.sh` — build `WordCount.jar`  
> 2. `python run_experiment.py` — run the experiments and collect CSV metrics  
> 3. `python stress_test.py` — run the failure injection / fault-recovery stress test  

---


---

## What the main scripts do

- `compile.sh`  
  Compiles `WordCount.java` and packages the classes into `WordCount.jar`. Required before running Hadoop jobs that use this jar.

- `start_hadoop.sh` / `stop_hadoop.sh`  
  Convenience scripts to control the Hadoop cluster used during experiments.

- `run_experiment.py`  
  Launches the benchmarking experiments (Hadoop workloads) across configurations (different core counts) and collects per-experiment metrics into CSV files. Produces CSVs that include runtime and average resource usage.

- `stress_test.py`  
  Runs a stress/fault test that samples host metrics over time and injects a failure at ~50% job completion by killing the NodeManager. Produces a time-series CSV of system stats plus an `event` column documenting the injected fault (`TASK_KILLED`).

## How to reproduce (exact commands)

1. Build Java jar:
```bash
./compile.sh
```

2. Start Hadoop components using the bash script.
```bash
./start_hadoop.sh
```

3. default: runs configured experiments and writes results to CSV files
```bash
python3 run_experiment.py
```

4. Inject fault by stopping the nodemanager at 50% task completion and collect time series metrics.
```bash
python3 stress_test.py
```

5. Stop the Hadoop components using the bash script.
```bash
./stop_hadoop.sh
```

## Dependencies 

- Python 3.12
- Java JDK 11 (to compile `WordCount.java` via `compile.sh`)
- Hadoop/YARN cluster accessible from the machine running experiments
