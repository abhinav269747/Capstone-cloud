"""
Workload and request generator for the simulator.
Creates realistic, time-varying workload patterns.
"""

import random
from typing import List, Iterator
from src.models import WorkloadRequest, VirtualMachine


class WorkloadGenerator:
    """
    Generates arriving requests according to configurable patterns.
    Supports Poisson arrivals, bursty patterns, and time-varying load.
    """

    def __init__(
        self,
        arrival_rate: float,
        vm_profiles: dict,
        service_time_mean: float,
        service_time_std: float,
        random_seed: int = 42,
    ):
        """
        Args:
            arrival_rate: Average requests per time unit.
            vm_profiles: Dict mapping profile names to resource requirements.
            service_time_mean: Mean service time for VMs.
            service_time_std: Standard deviation of service time.
            random_seed: For reproducibility.
        """
        self.arrival_rate = arrival_rate
        self.vm_profiles = vm_profiles
        self.service_time_mean = service_time_mean
        self.service_time_std = service_time_std
        self.random = random.Random(random_seed)
        self.request_counter = 0

    def generate_arrivals(
        self, simulation_time: float, pattern: str = "poisson"
    ) -> Iterator[tuple]:
        """
        Generate request arrivals over time.

        Args:
            simulation_time: Total simulation duration.
            pattern: 'poisson' (uniform random), 'bursty', or 'time_varying'.

        Yields:
            (arrival_time, WorkloadRequest)
        """
        current_time = 0
        self.request_counter = 0

        while current_time < simulation_time:
            # Inter-arrival time
            if pattern == "poisson":
                inter_arrival = self.random.expovariate(self.arrival_rate / 100)
            elif pattern == "bursty":
                # Alternate between sparse and dense arrivals
                burst_cycle = 1000  # Time units per cycle
                phase = (current_time % burst_cycle) / burst_cycle
                if phase < 0.3:
                    inter_arrival = self.random.expovariate(self.arrival_rate / 50)  # Sparse
                else:
                    inter_arrival = self.random.expovariate(self.arrival_rate / 10)  # Dense
            else:  # time_varying
                # Load increases over time (simulating growth)
                growth_factor = 1 + (current_time / simulation_time) * 0.5
                rate = self.arrival_rate * growth_factor
                inter_arrival = self.random.expovariate(rate / 100)

            current_time += inter_arrival
            if current_time >= simulation_time:
                break

            # Create request
            request_id = f"req_{self.request_counter}"
            self.request_counter += 1
            profile = self._choose_profile()
            request = WorkloadRequest(
                request_id=request_id,
                arrival_time=current_time,
                vm_profile=profile,
                vm_count=self._choose_vm_count(),
                priority=self.random.randint(1, 5),
            )

            yield current_time, request

    def create_vms_from_request(
        self, request: WorkloadRequest, max_sla_time: float
    ) -> List[VirtualMachine]:
        """
        Convert a request into one or more VMs with resource specs.

        Args:
            request: The incoming request.
            max_sla_time: Maximum time before SLA deadline.

        Returns:
            List of VirtualMachine objects.
        """
        vms = []
        profile = self.vm_profiles[request.vm_profile]

        for i in range(request.vm_count):
            service_time = max(
                self.service_time_mean
                + self.random.gauss(0, self.service_time_std),
                10,  # Minimum service time
            )
            vm = VirtualMachine(
                vm_id=f"{request.request_id}_vm{i}",
                required_cpu=profile["cpu"],
                required_gpu=profile["gpu"],
                required_ram=profile["ram"],
                requires_gpu=profile["requires_gpu"],
                service_time=service_time,
                arrival_time=request.arrival_time,
                required_io_bandwidth_gbps=profile.get("io_bandwidth_gbps", 0.0),
                required_io_iops_k=profile.get("io_iops_k", 0.0),
                priority=request.priority,
                sla_deadline=request.arrival_time + max_sla_time,
            )
            vms.append(vm)

        return vms

    def _choose_profile(self) -> str:
        """Randomly choose a VM profile (weighted by likelihood)."""
        profiles = list(self.vm_profiles.keys())
        # Light workloads are more common than heavy ones
        if self.random.random() < 0.6:
            return self.random.choice(profiles[:2])  # cpu_light, cpu_medium
        return self.random.choice(profiles)

    def _choose_vm_count(self) -> int:
        """Randomly choose how many VMs per request (usually 1-3)."""
        if self.random.random() < 0.7:
            return 1
        elif self.random.random() < 0.9:
            return 2
        return 3


class TraceReplayWorkloadGenerator(WorkloadGenerator):
    """
    Replays task arrivals from batch_task.csv for RLPA pre-training episodes.

    Overrides generate_arrivals() to read time-ordered tasks from the Alibaba
    trace instead of sampling synthetic inter-arrival times.  All other
    WorkloadGenerator helpers (create_vms_from_request, etc.) are reused as-is.
    """

    def __init__(
        self,
        trace_csv_path: str,
        vm_profiles: dict,
        sim_duration: float = 5000.0,
        max_tasks_per_episode: int = 800,
        service_time_mean: float = 100.0,
        service_time_std: float = 30.0,
        random_seed: int = 42,
    ) -> None:
        super().__init__(
            arrival_rate=5.0,           # not used in this subclass
            vm_profiles=vm_profiles,
            service_time_mean=service_time_mean,
            service_time_std=service_time_std,
            random_seed=random_seed,
        )
        self.trace_csv_path = trace_csv_path
        self.sim_duration = sim_duration
        self.max_tasks_per_episode = max_tasks_per_episode

    def generate_arrivals(self, simulation_time: float, pattern: str = "trace"):
        """Yield (arrival_time, WorkloadRequest) directly from the trace file."""
        from src.trace_reader import TraceReader

        reader = TraceReader(
            self.trace_csv_path,
            sim_duration=simulation_time,
            max_sim_cpu_cores=64,
            max_sim_ram_gb=256,
            max_tasks=self.max_tasks_per_episode,
        )
        self.request_counter = 0
        for task in reader.iter_sorted():
            if task.arrival_time >= simulation_time:
                break
            # Cap vm_count so we do not flood the cluster per trace task.
            vm_count = min(max(1, task.instance_num), 3)
            request = WorkloadRequest(
                request_id=f"trace_req_{self.request_counter}",
                arrival_time=task.arrival_time,
                vm_profile=task.vm_profile,
                vm_count=vm_count,
                priority=3,
            )
            self.request_counter += 1
            yield task.arrival_time, request
