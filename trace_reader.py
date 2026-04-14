"""
Trace reader for Alibaba cluster-trace-v2018 batch_task.csv.

Works with:
  - The real data (if downloaded): batch_task.csv from cluster-trace-v2018
  - The synthetic trace produced by data/generate_synthetic_trace.py

Schema (from v2018 schema.txt):
  task_name, instance_num, job_name, task_type, status,
  start_time, end_time, plan_cpu, plan_mem

Output: a time-ordered list of WorkloadRequest objects ready to feed
        into the SimPy simulation engine (trace-replay mode).

Mapping rules
─────────────
plan_cpu  (100 = 1 core) → required_cpu_cores
  Divided by 100 to get core count, then mapped against sim server range.
  Clamped to [0.5, MAX_CPU_CORES_IN_SIM].

plan_mem  ([0, 100] normalized) → required_ram_gb
  Scaled to [0.5 GB, MAX_RAM_GB_IN_SIM] using a linear map:
    required_ram_gb = plan_mem / 100 * MAX_RAM_GB_IN_SIM

start_time (seconds) → arrival_time (simulation time units)
  Traces are rescaled from [0, TRACE_DURATION_SEC] → [0, SIM_DURATION].

duration (end_time - start_time, seconds)
  → service_time (simulation time units) via the same scale factor.

Tasks with status != "Terminated" are skipped by default (configurable).
"""

import csv
import os
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

# ── public dataclass returned by the reader ───────────────────────────────────

@dataclass
class TraceTask:
    """A single task record parsed from batch_task.csv."""
    task_name: str
    job_name: str
    task_type: str
    status: str
    start_time_sec: float       # raw trace time (seconds)
    end_time_sec: float         # raw trace time (seconds)
    duration_sec: float         # end - start, always ≥ 0
    plan_cpu_units: float       # raw plan_cpu (100 = 1 core)
    plan_mem_norm: float        # raw plan_mem ([0, 100])
    instance_num: int

    # Mapped simulation fields (populated by TraceReader.map_to_sim)
    arrival_time: float = 0.0
    service_time: float = 0.0
    required_cpu_cores: float = 0.0
    required_ram_gb: float = 0.0

    # VM profile assigned during mapping
    vm_profile: str = "cpu_light"


# ── reader class ──────────────────────────────────────────────────────────────

class TraceReader:
    """
    Reads batch_task.csv and maps trace fields to simulation parameters.

    Parameters
    ----------
    csv_path : str
        Path to batch_task.csv (real or synthetic).
    sim_duration : float
        Total simulation time in sim time units.
    max_sim_cpu_cores : int
        Maximum CPU cores available on any single server in the sim.
    max_sim_ram_gb : int
        Maximum RAM (GB) available on any single server in the sim.
    skip_failed : bool
        If True (default), skip tasks where status != "Terminated".
    min_duration_sec : int
        Minimum raw task duration to include (filters tiny noise tasks).
    max_tasks : Optional[int]
        If set, stop after loading this many tasks (useful for quick runs).
    time_scale : Optional[float]
        If provided, overrides automatic rescaling.  Simulation time units
        per trace second.
    """

    # Column indices in v2018 batch_task.csv
    _COL = {
        "task_name":    0,
        "instance_num": 1,
        "job_name":     2,
        "task_type":    3,
        "status":       4,
        "start_time":   5,
        "end_time":     6,
        "plan_cpu":     7,
        "plan_mem":     8,
    }

    # v2018 full trace span (8 days in seconds)
    TRACE_DURATION_SEC: float = 8 * 24 * 3600  # 691 200

    def __init__(
        self,
        csv_path: str,
        sim_duration: float = 10_000.0,
        max_sim_cpu_cores: int = 32,
        max_sim_ram_gb: int = 128,
        skip_failed: bool = True,
        min_duration_sec: int = 5,
        max_tasks: Optional[int] = None,
        time_scale: Optional[float] = None,
    ) -> None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(
                f"Trace file not found: {csv_path}\n"
                "Run data/generate_synthetic_trace.py to create it first."
            )
        self.csv_path = csv_path
        self.sim_duration = sim_duration
        self.max_sim_cpu_cores = max_sim_cpu_cores
        self.max_sim_ram_gb = max_sim_ram_gb
        self.skip_failed = skip_failed
        self.min_duration_sec = min_duration_sec
        self.max_tasks = max_tasks

        # Compute time rescaling factor
        if time_scale is not None:
            self._time_scale = time_scale
        else:
            self._time_scale = sim_duration / self.TRACE_DURATION_SEC

    # ── public API ────────────────────────────────────────────────────────────

    def load(self) -> List[TraceTask]:
        """
        Load all (or up to max_tasks) tasks into a time-sorted list.
        Returns the list of TraceTask objects with simulation fields set.
        """
        tasks: List[TraceTask] = []
        for task in self._iter_rows():
            tasks.append(task)
        # Sort by mapped arrival_time for deterministic replay
        tasks.sort(key=lambda t: t.arrival_time)
        return tasks

    def iter_sorted(self) -> Iterator[TraceTask]:
        """Memory-efficient: loads all, yields in arrival_time order."""
        yield from self.load()

    # ── internal ──────────────────────────────────────────────────────────────

    def _iter_rows(self) -> Iterator[TraceTask]:
        """Parse CSV rows one by one and yield mapped TraceTask objects."""
        count = 0
        c = self._COL
        with open(self.csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # skip header
            try:
                header = next(reader)
            except StopIteration:
                return

            for row in reader:
                if len(row) < 9:
                    continue  # malformed row

                try:
                    status = row[c["status"]].strip()
                    if self.skip_failed and status != "Terminated":
                        continue

                    start_sec = float(row[c["start_time"]])
                    end_sec = float(row[c["end_time"]])
                    plan_cpu = float(row[c["plan_cpu"]])
                    plan_mem = float(row[c["plan_mem"]])
                except (ValueError, IndexError):
                    continue  # skip unparseable rows

                duration_sec = max(0.0, end_sec - start_sec)
                if duration_sec < self.min_duration_sec:
                    continue

                # Skip invalid plan_cpu / plan_mem
                if plan_cpu <= 0 or plan_mem <= 0:
                    continue

                task = TraceTask(
                    task_name=row[c["task_name"]].strip(),
                    job_name=row[c["job_name"]].strip(),
                    task_type=row[c["task_type"]].strip(),
                    status=status,
                    start_time_sec=start_sec,
                    end_time_sec=end_sec,
                    duration_sec=duration_sec,
                    plan_cpu_units=plan_cpu,
                    plan_mem_norm=max(0.0, min(100.0, plan_mem)),
                    instance_num=max(1, int(float(row[c["instance_num"]]))),
                )
                self._map_to_sim(task)
                yield task

                count += 1
                if self.max_tasks is not None and count >= self.max_tasks:
                    return

    def _map_to_sim(self, task: TraceTask) -> None:
        """Fill simulation fields in-place from raw trace fields."""
        ts = self._time_scale

        # Arrival time: rescale trace start_time → sim time units
        task.arrival_time = task.start_time_sec * ts

        # Service time: rescale duration → sim time units, floor at 1 unit
        task.service_time = max(1.0, task.duration_sec * ts)

        # CPU: plan_cpu units / 100 → cores, clamped to server capacity
        raw_cores = task.plan_cpu_units / 100.0
        task.required_cpu_cores = max(0.5, min(float(self.max_sim_cpu_cores), raw_cores))

        # RAM: plan_mem (normalized 0-100) → GB fraction of max server RAM
        task.required_ram_gb = max(
            0.5,
            min(float(self.max_sim_ram_gb), task.plan_mem_norm / 100.0 * self.max_sim_ram_gb)
        )

        # Assign a VM profile based on resource demands
        task.vm_profile = _classify_vm_profile(task.required_cpu_cores, task.required_ram_gb)


# ── VM profile classifier ─────────────────────────────────────────────────────

def _classify_vm_profile(cpu_cores: float, ram_gb: float) -> str:
    """
    Map (cpu_cores, ram_gb) onto one of the simulator's VM profile names.

    Thresholds are chosen to distribute tasks across profiles in roughly
    the proportions observed in published Alibaba trace analyses:
      ~55% cpu_light, ~25% cpu_medium, ~15% cpu_heavy, ~5% io_light
    (No GPU tasks from batch_task — those come from container traces.)
    """
    if cpu_cores <= 1.5:
        return "cpu_light"
    if cpu_cores <= 4.0:
        if ram_gb > 8.0:
            return "io_light"    # memory-hungry small CPU → I/O-light profile
        return "cpu_medium"
    if cpu_cores <= 8.0:
        return "cpu_heavy"
    return "cpu_heavy"           # anything larger maps to heaviest CPU profile


# ── convenience summary ───────────────────────────────────────────────────────

def print_trace_summary(tasks: List[TraceTask]) -> None:
    """Print a concise breakdown of a loaded trace list."""
    if not tasks:
        print("No tasks loaded.")
        return

    durations = [t.service_time for t in tasks]
    cpus = [t.required_cpu_cores for t in tasks]
    rams = [t.required_ram_gb for t in tasks]
    arrivals = [t.arrival_time for t in tasks]
    profiles: dict = {}
    for t in tasks:
        profiles[t.vm_profile] = profiles.get(t.vm_profile, 0) + 1

    n = len(tasks)
    print(f"─── Trace Summary ────────────────────────────────")
    print(f"  Tasks loaded       : {n:,}")
    print(f"  Arrival range      : [{min(arrivals):.1f}, {max(arrivals):.1f}] sim-units")
    print(f"  Service time  avg  : {sum(durations)/n:.2f}  min: {min(durations):.2f}  max: {max(durations):.2f}")
    print(f"  CPU cores     avg  : {sum(cpus)/n:.2f}  min: {min(cpus):.2f}  max: {max(cpus):.2f}")
    print(f"  RAM (GB)      avg  : {sum(rams)/n:.2f}  min: {min(rams):.2f}  max: {max(rams):.2f}")
    print(f"  VM profile breakdown:")
    for pname, cnt in sorted(profiles.items(), key=lambda x: -x[1]):
        print(f"    {pname:<18} {cnt:>6,}  ({100*cnt/n:.1f}%)")
    print(f"──────────────────────────────────────────────────")


# ── self-test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    default_csv = os.path.join(os.path.dirname(__file__), "..", "data", "batch_task.csv")
    csv_path = sys.argv[1] if len(sys.argv) > 1 else default_csv

    reader = TraceReader(
        csv_path=csv_path,
        sim_duration=10_000,
        max_sim_cpu_cores=32,
        max_sim_ram_gb=128,
        max_tasks=2_000,
    )
    tasks = reader.load()
    print_trace_summary(tasks)
