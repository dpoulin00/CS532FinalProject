# Hadoop Benchmarking on Word Count

This repository contains code, data and plots used to benchmark MapReduce (Hadoop) 

Use python to run the WordCount-V2.py script.

---


---

## What the main scripts do

- `WordCount-V2.py`  
  Our main pyspark testing script.


## How to reproduce (exact commands)

In WordCount-V2.py, change the main() method call's arguments in line 621.

To reproduce baseline results, set node_failure_policy to 0 for all core numbers, num_partitions to 200, and mem_frac to 0.6.
Other arguments adjusted as corresponding to each test.

To reproduce node failure tests, from base settings, change the core_failure_policy to have a 1 for all core numbers.

To reproduce disk spill test, from base settings, change mem_frac to 128/(15000 - 300)

To repro
```

## Dependencies 

- Python 3.12
- pyspark

