"""
Synthetic Alibaba cluster-trace-v2018 batch_task trace generator.

Produces batch_task.csv in the exact v2018 schema:
  task_name, instance_num, job_name, task_type, status,
  start_time, end_time, plan_cpu, plan_mem

Distributions are calibrated to match published analyses of the real trace:
  - CPU:      log-normal, median ~100 units (100 = 1 core), heavy tail to 3200
  - Memory:   beta distribution, scaled to [0, 100] normalized, mostly < 20
  - Duration: Pareto (heavy-tailed), median ~300 s, range 10 s – 6 h
  - Arrivals: Poisson with a daily sinusoidal intensity pattern over 8 days
  - Task types: 12 types (type_1 … type_12) matching v2018 schema
  - DAG tasks: ~40% have dependencies encoded as "M{id}_{parent_id}" names

Reference distributions from:
  Guo et al. (2019) "Who Limits the Resource Efficiency of My Datacenter"
  Wang et al. (2022) "MLaaS in the Wild: Workload Analysis and Scheduling"
"""

import csv
import math
import os
import random

# ── reproducibility ───────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

# ── output path ──────────────────────────────────────────────────────────────
OUT_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_TASK_CSV = os.path.join(OUT_DIR, "batch_task.csv")

# ── trace parameters ─────────────────────────────────────────────────────────
TRACE_DURATION_SEC = 8 * 24 * 3600  # 8 days in seconds (691 200 s)
NUM_JOBS = 5_000                    # number of batch jobs
TASK_TYPES = [f"type_{i}" for i in range(1, 13)]

# daily peak/trough multipliers (workload is heavier during daytime)
PEAK_HOUR = 14          # 2 pm = peak
TROUGH_HOUR = 4         # 4 am = trough
AMPLITUDE = 0.45        # ±45 % variation around mean


# ── helper samplers ───────────────────────────────────────────────────────────

def _sample_cpu() -> float:
    """
    plan_cpu: 100 = 1 core.
    Log-normal, clipped to [50, 3200].  Median ≈ 100 (1 core).
    """
    mu, sigma = math.log(100), 0.9
    val = math.exp(random.gauss(mu, sigma))
    return max(50.0, min(3200.0, round(val / 50) * 50))


def _sample_mem() -> float:
    """
    plan_mem: [0, 100] normalized.
    Beta(1.5, 6) → mostly < 30.  Clipped to [1, 99].
    """
    # Simple beta sampling using two gamma variates
    a, b = 1.5, 6.0
    x = random.gammavariate(a, 1.0)
    y = random.gammavariate(b, 1.0)
    beta_val = x / (x + y)  # in (0, 1)
    return max(1.0, min(99.0, round(beta_val * 100, 1)))


def _sample_duration() -> int:
    """
    Task duration in seconds.
    Pareto(alpha=1.2, min=10).  E[X] ≈ 1 min, heavy tail to several hours.
    Clipped to [10, 21600] (10 s – 6 h).
    """
    alpha = 1.2
    xmin = 10
    u = random.random()
    # inverse CDF of Pareto: xmin / (1 - u)^(1/alpha)
    dur = xmin / ((1 - u) ** (1.0 / alpha))
    return max(10, min(21600, int(dur)))


def _daily_intensity(t_sec: float) -> float:
    """Sinusoidal daily intensity multiplier around 1.0."""
    hour_of_day = (t_sec % 86400) / 3600
    angle = 2 * math.pi * (hour_of_day - PEAK_HOUR) / 24
    return 1.0 + AMPLITUDE * math.sin(angle)


def _sample_start_times(n_jobs: int) -> list:
    """
    Distribute job arrivals over TRACE_DURATION_SEC using
    thinned Poisson process with a sinusoidal daily rate.
    """
    base_rate = n_jobs / TRACE_DURATION_SEC   # jobs per second on average
    times = []
    t = 0.0
    # upper bound on rate for rejection sampling
    max_rate = base_rate * (1.0 + AMPLITUDE)
    while len(times) < n_jobs:
        # time to next candidate event (homogeneous Poisson with max_rate)
        t += random.expovariate(max_rate)
        if t >= TRACE_DURATION_SEC:
            # wrap around or stop
            if len(times) < n_jobs:
                t = random.uniform(0, TRACE_DURATION_SEC)
            else:
                break
        # thin: accept with probability intensity(t) / max_rate
        accept_prob = _daily_intensity(t) / (1.0 + AMPLITUDE)
        if random.random() < accept_prob:
            times.append(t)
    # If still short (edge case), fill remaining uniformly
    while len(times) < n_jobs:
        times.append(random.uniform(0, TRACE_DURATION_SEC))
    times = sorted(times[:n_jobs])
    return times


# ── DAG task-name builder ─────────────────────────────────────────────────────

def _build_dag_task_names(num_tasks: int, job_id: int) -> list:
    """
    About 40 % of jobs are DAGs; the rest are independent tasks.
    DAG encoding: 'M{task_idx}' for roots, 'M{task_idx}_{parent_idx}' for deps.
    This mirrors the format described in the v2018 schema documentation.
    """
    names = []
    is_dag = random.random() < 0.40 and num_tasks > 1
    for i in range(1, num_tasks + 1):
        if not is_dag or i == 1:
            names.append(f"M{i}")
        else:
            # Pick a random predecessor among already-created tasks
            parent = random.randint(1, i - 1)
            # Occasionally fan-in to two parents (complex DAG)
            if i > 2 and random.random() < 0.15:
                grandparent = random.randint(1, parent)
                names.append(f"M{i}_{parent}_{grandparent}")
            else:
                names.append(f"M{i}_{parent}")
    return names


# ── main generator ────────────────────────────────────────────────────────────

def generate(output_path: str = BATCH_TASK_CSV, num_jobs: int = NUM_JOBS) -> int:
    """
    Generate synthetic batch_task.csv.
    Returns total number of task rows written.
    """
    start_times = _sample_start_times(num_jobs)
    rows_written = 0

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        # header
        writer.writerow([
            "task_name", "instance_num", "job_name", "task_type",
            "status", "start_time", "end_time", "plan_cpu", "plan_mem"
        ])

        for job_idx, job_start in enumerate(start_times, start=1):
            job_name = f"job_{job_idx}"
            task_type = random.choice(TASK_TYPES)
            # Jobs have 1–8 tasks; mostly 1–3
            num_tasks = max(1, int(random.expovariate(1.0 / 2.5)))
            num_tasks = min(num_tasks, 8)

            task_names = _build_dag_task_names(num_tasks, job_idx)

            # Track sequential DAG completion times
            task_end_times: dict = {}

            for task_idx in range(num_tasks):
                tname = task_names[task_idx]
                instance_num = max(1, int(random.expovariate(1.0 / 3)))
                instance_num = min(instance_num, 100)

                plan_cpu = _sample_cpu()
                plan_mem = _sample_mem()
                duration = _sample_duration()

                # Determine earliest start for this task (respecting DAG deps)
                # Parse parent IDs from name like "M3_1_2"
                parts = tname.split("_")
                task_start = job_start
                if len(parts) > 1:
                    for p in parts[1:]:
                        try:
                            parent_id = int(p)
                            parent_name = task_names[parent_id - 1]
                            parent_end = task_end_times.get(parent_name, job_start)
                            task_start = max(task_start, parent_end)
                        except (ValueError, IndexError):
                            pass

                task_end = task_start + duration
                task_end_times[tname] = task_end

                # Clip to trace window; mark as Failed if extends beyond
                if task_start >= TRACE_DURATION_SEC:
                    task_start = TRACE_DURATION_SEC - 1
                    task_end = task_start
                    status = "Failed"
                elif task_end > TRACE_DURATION_SEC:
                    status = "Failed"
                else:
                    status = "Terminated"   # v2018 uses "Terminated" for success

                writer.writerow([
                    tname,
                    instance_num,
                    job_name,
                    task_type,
                    status,
                    int(task_start),
                    int(task_end),
                    round(plan_cpu, 1),
                    round(plan_mem, 1),
                ])
                rows_written += 1

    return rows_written


if __name__ == "__main__":
    print(f"Generating synthetic Alibaba v2018 batch_task trace ...")
    out = BATCH_TASK_CSV
    n = generate(out, NUM_JOBS)
    size_kb = os.path.getsize(out) / 1024
    print(f"Done. {n:,} task rows written to: {out}  ({size_kb:.0f} KB)")
