# Practical Apache Spark & PySpark Learning — Step-by-Step Lab

> **Purpose:** This document records the practical Spark setup and execution steps we actually performed while building a local Spark Standalone cluster and running PySpark jobs.
>
> **Environment:** Windows 11 + WSL 2 + Docker Desktop + VS Code + Apache Spark 3.5.5
>
> **Important:** This is a local Spark learning setup. No Azure, Databricks, ADF, ADLS, or cloud services are required.

---

## 1. What We Built

We built a real local Spark Standalone cluster:

```text
                    Spark Master
                  spark-master:7077
                         |
              -----------------------
              |                     |
        Spark Worker 1        Spark Worker 2
          2 CPU cores           2 CPU cores
            1 GB RAM              1 GB RAM
              |                     |
           Executor              Executor
```

### Current cluster capacity

| Component | Configuration |
|---|---|
| Spark version | 3.5.5 |
| Master | 1 |
| Workers | 2 |
| Worker 1 cores | 2 |
| Worker 2 cores | 2 |
| Total cores | 4 |
| Worker memory | 1 GB each |
| Total worker memory | 2 GB |
| Container platform | Docker |
| Host OS | Windows |
| Project drive | `D:` |

---

# 2. Prerequisites

We used:

- Windows 11
- WSL 2
- Ubuntu
- Docker Desktop
- VS Code
- Docker Compose
- Apache Spark 3.5.5

---

# 3. Verify WSL 2

From Windows terminal:

```powershell
wsl -l -v
```

Expected:

```text
NAME              STATE           VERSION
Ubuntu            Stopped         2
docker-desktop    Running         2
```

Then:

```powershell
wsl --status
```

The important point was:

```text
Default Version: 2
```

We also verified in Task Manager that:

```text
Virtualization: Enabled
```

---

# 4. Verify Docker

Docker Desktop was installed and working.

We tested Docker using:

```powershell
docker run hello-world
```

The successful `hello-world` output confirmed that Docker was working.

---

# 5. Create the Spark Learning Project

We wanted the project on the `D:` drive rather than `C:`.

Project directory:

```text
D:\spark-learning
```

Inside it:

```text
D:\spark-learning
│
├── notebooks\
├── jobs\
└── data\
```

### Purpose of each folder

| Folder | Purpose |
|---|---|
| `jobs` | PySpark Python scripts |
| `data` | CSV and other input/output data |
| `notebooks` | Future notebooks/experiments |

---

# 6. Create Docker Compose Configuration

Inside:

```text
D:\spark-learning\docker-compose.yml
```

we created the Spark cluster configuration.

```yaml
services:

  spark-master:
    image: apache/spark:3.5.5
    container_name: spark-master
    hostname: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host spark-master --port 7077 --webui-port 8080
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - ./jobs:/opt/spark/jobs
      - ./data:/opt/spark/data
    networks:
      - spark-network

  spark-worker-1:
    image: apache/spark:3.5.5
    container_name: spark-worker-1
    hostname: spark-worker-1
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --cores 2 --memory 1G --webui-port 8081
    depends_on:
      - spark-master
    ports:
      - "8081:8081"
    volumes:
      - ./jobs:/opt/spark/jobs
      - ./data:/opt/spark/data
    networks:
      - spark-network

  spark-worker-2:
    image: apache/spark:3.5.5
    container_name: spark-worker-2
    hostname: spark-worker-2
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --cores 2 --memory 1G --webui-port 8082
    depends_on:
      - spark-master
    ports:
      - "8082:8082"
    volumes:
      - ./jobs:/opt/spark/jobs
      - ./data:/opt/spark/data
    networks:
      - spark-network

networks:
  spark-network:
    driver: bridge
```

---

# 7. Validate Docker Compose

Before starting the cluster:

```powershell
docker compose config
```

A valid configuration confirmed that the YAML could be parsed.

---

# 8. Start the Spark Cluster

From:

```powershell
D:\spark-learning
```

run:

```powershell
docker compose up -d
```

If containers were already running and we wanted a clean restart:

```powershell
docker compose down
docker compose up -d
```

---

# 9. Verify Running Containers

Run:

```powershell
docker ps
```

We verified that these three containers were running:

```text
spark-master
spark-worker-1
spark-worker-2
```

The architecture was now:

```text
Windows
  |
Docker Desktop
  |
  +-- spark-master
  |
  +-- spark-worker-1
  |
  +-- spark-worker-2
```

---

# 10. Open Spark Master UI

Open in a browser:

```text
http://localhost:8080
```

The Spark Master UI showed:

```text
Spark Version: 3.5.5
Alive Workers: 2
Total Cores: 4
Total Worker Memory: 2 GiB
```

Workers shown:

```text
Worker 1 → 2 cores → 1 GiB
Worker 2 → 2 cores → 1 GiB
```

This was the first practical confirmation that our Spark cluster was actually working.

---

# 11. Understand the Docker Volume Mapping

The Windows project:

```text
D:\spark-learning\jobs
```

is mounted inside the container as:

```text
/opt/spark/jobs
```

Similarly:

```text
D:\spark-learning\data
```

is mounted as:

```text
/opt/spark/data
```

So:

```text
Windows
D:\spark-learning\data\orders.csv
             |
             | Docker volume
             ↓
Container
/opt/spark/data/orders.csv
```

### Important

When Spark runs **inside the Docker container**, it uses:

```text
/opt/spark/data/orders.csv
```

not:

```text
D:\spark-learning\data\orders.csv
```

---

# 12. Verify the Data Folder Inside the Container

We checked the mounted data directory using:

```powershell
docker exec spark-master ls /opt/spark/data
```

This confirmed that files from the Windows `data` folder were visible inside the Spark container.

---

# 13. Create the First PySpark Job

Inside:

```text
D:\spark-learning\jobs
```

we created:

```text
hellospark.py
```

The intended code was:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.csv(
    "/opt/spark/data/orders.csv",
    header=True,
    inferSchema=True
)

df.show(5)

spark.stop()
```

---

# 14. Understand the SparkSession Code

The important builder sequence is:

```python
spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
```

### Meaning

```text
SparkSession.builder
        ↓
Set application name
        ↓
Set Spark Master
        ↓
getOrCreate()
        ↓
SparkSession
```

### Important

`getOrCreate()` should be called at the end of the builder chain.

---

# 15. The Mistake We Made

We initially had code equivalent to:

```python
spark = SparkSession.builder \
    .appName("Hello Spark").getOrCreate() \
    .master("spark://spark-master:7077") \
    .getOrCreate()
```

This caused:

```text
AttributeError:
'SparkSession' object has no attribute 'master'
```

### Why?

Because the first:

```python
.getOrCreate()
```

already returned a `SparkSession`.

Then the code effectively tried:

```python
SparkSession.master(...)
```

But `.master()` belongs to the **builder**, not the already-created SparkSession.

### Correct:

```python
SparkSession.builder \
    .appName("Hello Spark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
```

---

# 16. Another Small Error We Encountered

We also encountered a capitalization mistake:

```python
.appname(...)
```

The correct method is:

```python
.appName(...)
```

Python method names are case-sensitive.

---

# 17. Submit the PySpark Job

We ran the job from the Spark master container:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/hellospark.py
```

### What this command means

```text
docker exec
   ↓
Run a command inside spark-master container

spark-submit
   ↓
Submit a Spark application

--master spark://spark-master:7077
   ↓
Use our Spark Master

/opt/spark/jobs/hellospark.py
   ↓
Run our Python Spark job
```

---

# 18. What Happened During Job Submission

Even when the Python code contained the builder error, the Spark logs proved that the cluster connection itself worked.

The logs showed:

```text
Submitted application: Hello Spark
```

and:

```text
Connected to Spark cluster
```

with an application ID similar to:

```text
app-20261008083649-0000
```

The logs also showed executors being added:

```text
Executor added ... with 2 core(s)
Executor added ... with 2 core(s)
```

This proved:

```text
Driver
   ↓
Spark Master
   ↓
Worker 1 → Executor
Worker 2 → Executor
```

was functioning.

The failure was in our Python code, not in Docker or the Spark cluster.

---

# 19. Harmless Spark Warning

During execution we saw a warning similar to:

```text
WARN NativeCodeLoader:
Unable to load native-hadoop library...
```

This was not the cause of our Python failure.

The actual failure was:

```text
AttributeError:
'SparkSession' object has no attribute 'master'
```

The important debugging lesson:

> Don't assume the first warning in the log is the root cause. Find the actual exception and traceback.

---

# 20. Why We Use `spark-submit`

For this learning setup, we should submit jobs through Spark rather than simply clicking the VS Code Python Run button.

### VS Code Python Run

```text
VS Code
 ↓
Local Windows Python
```

### spark-submit

```text
VS Code
 ↓
Docker
 ↓
Spark Master
 ↓
Workers
 ↓
Executors
```

The second path allows us to actually learn distributed Spark execution.

---

# 21. Reading a CSV in PySpark

Our code:

```python
df = spark.read.csv(
    "/opt/spark/data/orders.csv",
    header=True,
    inferSchema=True
)
```

### `header=True`

Tells Spark:

> The first row contains column names.

### `inferSchema=True`

Tells Spark:

> Inspect the input and infer appropriate column data types.

For example:

```text
id       → integer
amount   → double
name     → string
```

For production workloads, explicit schemas are often preferred because they provide stronger control over data types.

---

# 22. Display the Data

We used:

```python
df.show(5)
```

This is an **action**.

It triggers Spark execution.

The simplified execution flow is:

```text
spark.read.csv(...)
        ↓
DataFrame created
        ↓
df.show(5)
        ↓
ACTION
        ↓
JOB
        ↓
TASKS
        ↓
EXECUTORS
```

---

# 23. Stop the Spark Session

At the end:

```python
spark.stop()
```

This shuts down the Spark application/session cleanly.

---

# 24. What We Learned About Partitions

A partition is a chunk of data.

Example:

```text
100 GB DATA

+---------+---------+---------+---------+
| Part 1  | Part 2  | Part 3  | Part 4  |
+---------+---------+---------+---------+
```

Each partition can become work for a task.

Important:

```text
Partition = DATA
Task      = WORK
```

Do not confuse them.

---

# 25. What We Learned About Tasks

If a stage has:

```text
20 partitions
```

we generally have approximately:

```text
20 tasks
```

for that stage.

If the cluster has:

```text
4 available cores
```

then approximately:

```text
4 tasks
```

can run concurrently.

The remaining tasks wait for available capacity.

Example:

```text
20 Tasks
   |
   +-- Wave 1 → T1 T2 T3 T4
   +-- Wave 2 → T5 T6 T7 T8
   +-- Wave 3 → T9 T10 T11 T12
   +-- ...
```

---

# 26. Partitions Are Not Executors

A common misconception:

```text
20 partitions
≠
20 executors
```

Partitions represent data/work units.

Executors are processes that execute tasks.

The same executor can execute many tasks over time.

---

# 27. Our Current Cluster Example

Our local cluster has:

```text
Worker 1 → 2 cores
Worker 2 → 2 cores

Total → 4 cores
```

Suppose we have:

```text
20 partitions
```

Then:

```text
20 tasks
```

can be created for the relevant stage.

With 4 task slots:

```text
~4 tasks at once
```

The tasks execute in multiple waves.

---

# 28. Transformation vs Action

### Transformation

Examples:

```python
df.filter(...)
df.select(...)
df.withColumn(...)
df.groupBy(...)
```

Transformations are lazy.

They define what Spark should do.

They do not normally execute immediately.

### Action

Examples:

```python
df.show()
df.count()
df.collect()
df.write...
```

An action triggers execution.

Memory rule:

```text
Transformation = PLAN

Action = START
```

---

# 29. Job

An action triggers a Spark Job.

Example:

```python
df.filter(...)
df.select(...)
df.groupBy(...)

df.show()
```

The `show()` action causes Spark to execute the required work.

Think:

```text
Action
  ↓
Job
```

---

# 30. Stage

A job can be divided into stages.

A major stage boundary is typically associated with a shuffle.

Example:

```text
Stage 1
Read
 ↓
Filter
 ↓
Select
 ↓
Shuffle
 ↓
Stage 2
GroupBy
 ↓
Aggregation
```

Important:

> A `groupBy()` is lazy by itself, but it can introduce a shuffle when the plan is executed.

---

# 31. Shuffle

Shuffle means data is redistributed between partitions/executors.

Conceptually:

```text
Before shuffle:

Worker 1 → A A A
Worker 2 → B B B
Worker 3 → C C C

          ↓ SHUFFLE ↓

Worker 1 → A A
Worker 2 → B B
Worker 3 → C C
```

Shuffle can be expensive because it may involve:

- Data movement
- Network communication
- Reorganization
- Disk I/O/spill when needed

Operations such as many aggregations and joins can require shuffle depending on the execution plan.

---

# 32. Data Skew

We studied a simple example:

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 10 GB
```

The first three tasks may finish quickly.

The task processing the 10 GB partition can take much longer.

Visual:

```text
P1 → 1 GB  → ✅
P2 → 1 GB  → ✅
P3 → 1 GB  → ✅
P4 → 10 GB → 🐢
```

This is a form of data skew / uneven workload.

The long-running task can become a **straggler**.

---

# 33. Why More Cores Don't Automatically Fix Skew

We clarified that these are different resources:

```text
CPU cores = computation/concurrency

Memory = RAM available for processing

Disk = storage / possible spill location
```

Adding cores does not automatically split an already-running oversized task into smaller tasks.

Therefore:

```text
More cores
≠
Automatic solution to skew
```

Possible approaches include:

- Better partitioning
- Broadcast joins when appropriate
- Salting
- Adaptive Query Execution
- Shuffle tuning

The correct solution depends on the actual bottleneck.

---

# 34. How Production Engineers Think About Partitions

We discussed that partition counts should not be chosen randomly.

A simplified production thinking process is:

```text
Data Size
    +
Workload Type
    +
Cluster Resources
    +
Memory/CPU Needs
    +
Performance Requirement
        ↓
Initial Configuration
        ↓
Run Job
        ↓
Inspect Spark UI
        ↓
Find Bottleneck
        ↓
Tune
        ↓
Run Again
```

Important:

> Data determines the amount and shape of work. Cluster resources determine how much work can be processed concurrently.

---

# 35. Partition Size Is Not a Universal Number

We discussed an illustrative example of using:

```text
256 MB
```

as a partition-size target.

For example:

```text
100 GB / 0.25 GB ≈ 400 partitions
```

and:

```text
1 TB / 0.25 GB ≈ 4096 partitions
```

But this is only an example.

It is **not** a universal Spark production rule.

Spark's actual input partitioning depends on factors such as:

- File format
- File layout
- File sizes
- Spark configuration
- Workload
- Cluster resources

For example, Spark has a file partition configuration:

```python
spark.conf.set(
    "spark.sql.files.maxPartitionBytes",
    256 * 1024 * 1024
)
```

This explicitly sets the maximum number of bytes to consider when partitioning file-based input.

---

# 36. Input Partitions vs Shuffle Partitions

These should not be confused.

### Input partitioning

Concerned with how input data is divided for reading.

Example configuration:

```text
spark.sql.files.maxPartitionBytes
```

### Shuffle partitioning

Concerned with how data is partitioned after shuffle operations.

Example:

```text
spark.sql.shuffle.partitions
```

These are different concepts.

---

# 37. Practical Execution Mental Model

The most important model from the practical work:

```text
                 DRIVER
                   |
                JOB
                   |
              +----+----+
              |         |
           STAGE 1   STAGE 2
              |         |
           TASKS      TASKS
              |         |
          PARTITIONS PARTITIONS
              |
          EXECUTORS
              |
            CORES
```

With shuffle:

```text
Stage 1
   ↓
SHUFFLE
   ↓
Stage 2
```

---

# 38. Debugging Lessons From Our Practical Work

### Lesson 1 — Read the traceback

The Spark cluster was healthy, but our Python code was wrong.

### Lesson 2 — Check where the code runs

Running Python directly on Windows is different from submitting to Spark inside Docker.

### Lesson 3 — Understand container paths

Windows:

```text
D:\spark-learning\data
```

Container:

```text
/opt/spark/data
```

### Lesson 4 — Don't confuse warnings with errors

A warning does not necessarily mean the application failed.

Find the actual exception.

### Lesson 5 — Understand lazy execution

Creating transformations does not mean Spark immediately processes all data.

An action triggers the required execution.

---

# 39. Practical Checklist

Use this whenever starting the local Spark lab.

## Start cluster

```powershell
cd D:\spark-learning
docker compose up -d
```

## Verify containers

```powershell
docker ps
```

Expected:

```text
spark-master
spark-worker-1
spark-worker-2
```

## Check Spark UI

Open:

```text
http://localhost:8080
```

Verify:

```text
2 workers
4 total cores
2 GiB total worker memory
```

## Check files

```powershell
docker exec spark-master ls /opt/spark/data
```

## Submit PySpark job

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/hellospark.py
```

## Stop cluster when finished

```powershell
docker compose down
```

---

# 40. Current Learning Status

### Completed practically

- [x] WSL 2 setup
- [x] Ubuntu/WSL verification
- [x] Virtualization verification
- [x] Docker Desktop setup
- [x] Docker test
- [x] Spark project created on `D:`
- [x] Docker Compose created
- [x] Apache Spark 3.5.5 image configured
- [x] Spark Master created
- [x] Two Spark Workers created
- [x] Worker cores configured
- [x] Worker memory configured
- [x] Docker volumes configured
- [x] Spark Master UI verified
- [x] Data mounted into containers
- [x] PySpark job created
- [x] SparkSession configuration learned
- [x] `spark-submit` learned
- [x] Cluster connection verified from logs
- [x] CSV reading introduced
- [x] `header=True` understood
- [x] `inferSchema=True` understood
- [x] `show()` understood as an action
- [x] Partitions/tasks/cores relationship practiced
- [x] Transformations and lazy evaluation understood
- [x] Jobs and stages understood
- [x] Shuffle understood conceptually
- [x] Data skew understood
- [x] Basic production tuning mindset established

---

# 41. The Most Important Rules to Remember

```text
PARTITION = DATA CHUNK

TASK = WORK ON A PARTITION

CORE = CAPACITY TO RUN TASKS

EXECUTOR = PROCESS THAT RUNS TASKS

WORKER = MACHINE PROVIDING RESOURCES

DRIVER = COORDINATOR

TRANSFORMATION = LAZY PLAN

ACTION = TRIGGERS EXECUTION

JOB = EXECUTION TRIGGERED BY AN ACTION

STAGE = PART OF A JOB

SHUFFLE = DATA REDISTRIBUTION

SKEW = UNEVEN WORKLOAD
```

And the most important execution flow:

```text
PySpark Code
     ↓
Transformations
     ↓
Lazy Plan
     ↓
Action
     ↓
Job
     ↓
Stages
     ↓
Tasks
     ↓
Executors
     ↓
Cores
     ↓
Parallel Execution
```

---

# 42. Next Practical Learning Direction

The next step is to continue from the working Spark cluster into hands-on DataFrame operations.

Recommended progression:

```text
CSV Reading
    ↓
Schema
    ↓
select()
    ↓
filter()
    ↓
withColumn()
    ↓
drop()
    ↓
rename
    ↓
sort
    ↓
distinct / dropDuplicates
    ↓
Aggregations
    ↓
Joins
    ↓
Window Functions
    ↓
Data Cleaning
    ↓
Performance
```

The goal is not only to memorize PySpark syntax.

The goal is to understand:

```text
What code did I write?
        ↓
What transformation/action is it?
        ↓
What does Spark do with it?
        ↓
How is the work divided?
        ↓
Where could performance problems appear?
```

---

## Final Mental Model

When looking at a PySpark program, think in this order:

**1. What data am I processing?**

**2. How is that data partitioned?**

**3. What transformation am I defining?**

**4. What action triggers execution?**

**5. How does Spark divide the job into stages?**

**6. Where does shuffle happen?**

**7. How many tasks are created?**

**8. How many tasks can execute concurrently?**

**9. Are the tasks balanced?**

**10. What does the Spark UI tell me?**

That is the foundation for moving from simply **writing PySpark code** to **thinking like a Data Engineer working with Spark**.
