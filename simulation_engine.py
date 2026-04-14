"""
Base simulation engine using SimPy for discrete event simulation.
This is the core orchestrator for the cloud data center simulation.
"""

import simpy
import time
from threading import Lock
from typing import List, Dict, Callable, Optional, Set
from src.models import PhysicalServer, VirtualMachine, WorkloadRequest
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector


class CloudDataCenterSimulation:
    """
    Main simulation engine for the green cloud data center.
    Uses SimPy for discrete event simulation.
    """

    def __init__(
        self,
        servers: List[dict],
        vm_profiles: dict,
        workload_generator: WorkloadGenerator,
        scheduler: Callable,  # Placement algorithm (will be called at each decision point)
        max_sla_time: float,
        metrics_collector: MetricsCollector,
        random_seed: int = 42,
        enable_vm_migration: bool = False,
        migration_check_interval: float = 25.0,
        migration_source_max_util: float = 0.35,
        migration_dest_max_util: float = 0.85,
        max_migrations_per_vm: int = 2,
        vm_migration_cooldown: float = 120.0,
        server_migration_cooldown: float = 80.0,
        migration_base_downtime: float = 0.5,
        migration_bandwidth_gb_per_time: float = 8.0,
        migration_energy_overhead_kwh: float = 0.002,
        migration_energy_gain_horizon: float = 180.0,
        migration_min_net_energy_gain_kwh: float = 0.01,
        enable_server_power_states: bool = False,
        server_idle_shutdown_time: float = 120.0,
        server_power_state_check_interval: float = 10.0,
        server_wakeup_delay: float = 8.0,
        server_wakeup_energy_kwh: float = 0.01,
        progress_callback: Optional[Callable[[dict], None]] = None,
        realtime_delay_seconds: float = 0.0,
    ):
        """
        Args:
            servers: List of server config dicts.
            vm_profiles: VM resource profiles.
            workload_generator: Workload generator instance.
            scheduler: Function(env, pending_vms, servers) -> placement decisions.
            max_sla_time: Maximum time before SLA deadline.
            metrics_collector: Metrics tracking object.
            random_seed: For reproducibility.
        """
        self.env = simpy.Environment()
        self.servers: Dict[str, PhysicalServer] = {}
        self.pending_vms: List[VirtualMachine] = []
        self.running_vms: Dict[str, VirtualMachine] = {}
        self.completed_vms: List[VirtualMachine] = []
        self.vm_processes: Dict[str, simpy.events.Process] = {}
        self.migrating_vm_ids: Set[str] = set()
        self.waking_servers: Set[str] = set()
        self.vm_last_migration_time: Dict[str, float] = {}
        self.server_last_migration_time: Dict[str, float] = {}
        self.server_idle_since: Dict[str, Optional[float]] = {}

        self.workload_generator = workload_generator
        self.scheduler = scheduler
        self.max_sla_time = max_sla_time
        self.metrics = metrics_collector
        self.vm_profiles = vm_profiles
        self.enable_vm_migration = enable_vm_migration
        self.migration_check_interval = migration_check_interval
        self.migration_source_max_util = migration_source_max_util
        self.migration_dest_max_util = migration_dest_max_util
        self.max_migrations_per_vm = max_migrations_per_vm
        self.vm_migration_cooldown = vm_migration_cooldown
        self.server_migration_cooldown = server_migration_cooldown
        self.migration_base_downtime = migration_base_downtime
        self.migration_bandwidth_gb_per_time = migration_bandwidth_gb_per_time
        self.migration_energy_overhead_kwh = migration_energy_overhead_kwh
        self.migration_energy_gain_horizon = migration_energy_gain_horizon
        self.migration_min_net_energy_gain_kwh = migration_min_net_energy_gain_kwh
        self.enable_server_power_states = enable_server_power_states
        self.server_idle_shutdown_time = server_idle_shutdown_time
        self.server_power_state_check_interval = server_power_state_check_interval
        self.server_wakeup_delay = server_wakeup_delay
        self.server_wakeup_energy_kwh = server_wakeup_energy_kwh
        self.progress_callback = progress_callback
        self.realtime_delay_seconds = realtime_delay_seconds
        self.external_requests: List[WorkloadRequest] = []
        self.external_request_lock = Lock()
        self._external_request_counter = 0
        self.is_finished = False

        # Initialize physical servers
        for server_config in servers:
            server = PhysicalServer(**server_config)
            self.servers[server.name] = server
            self.server_idle_since[server.name] = None

        self.random_seed = random_seed

    def run(self, simulation_time: float, workload_pattern: str = "poisson", log_interval: float = 100):
        """
        Run the simulation.

        Args:
            simulation_time: Total time to simulate.
            workload_pattern: 'poisson', 'bursty', or 'time_varying'.
            log_interval: Interval for logging aggregate metrics.
        """
        print(f"\n{'='*60}")
        print(f"Starting simulation: {self.metrics.simulation_name}")
        print(f"Simulation time: {simulation_time}, Pattern: {workload_pattern}")
        print(f"Servers: {len(self.servers)}")
        print(f"{'='*60}\n")

        # Start processes
        self.env.process(self._arrival_process(simulation_time, workload_pattern))
        self.env.process(self._scheduling_process())
        self.env.process(self._metrics_logging_process(log_interval))
        self.env.process(self._external_request_process())
        if self.enable_vm_migration:
            self.env.process(self._migration_process())
        if self.enable_server_power_states:
            self.env.process(self._server_power_state_process())

        # Run
        self.env.run(until=simulation_time)

        # Finalize
        self._finalize_metrics()
        self.is_finished = True
        self._emit_progress("simulation_completed")
        self.metrics.print_summary()

    def _arrival_process(self, simulation_time: float, pattern: str):
        """
        Event source: generate incoming requests.
        """
        for arrival_time, request in self.workload_generator.generate_arrivals(
            simulation_time, pattern
        ):
            yield self.env.timeout(arrival_time - self.env.now)

            # Convert request to VMs
            vms = self.workload_generator.create_vms_from_request(request, self.max_sla_time)
            self.pending_vms.extend(vms)

            self.metrics.log_event(
                self.env.now,
                "request_arrival",
                {"request_id": request.request_id, "vm_count": len(vms)},
            )
            self._emit_progress("request_arrival", {"request_id": request.request_id, "vm_count": len(vms)})

            # Trigger scheduling
            yield self.env.timeout(0)  # Let other processes run

    def _external_request_process(self):
        """Poll API-injected workload requests and inject them into the sim at current time."""
        while True:
            injected_requests: List[WorkloadRequest] = []
            with self.external_request_lock:
                if self.external_requests:
                    injected_requests = list(self.external_requests)
                    self.external_requests.clear()

            for request in injected_requests:
                request.arrival_time = self.env.now
                vms = self.workload_generator.create_vms_from_request(request, self.max_sla_time)
                self.pending_vms.extend(vms)
                self.metrics.log_event(
                    self.env.now,
                    "external_request_arrival",
                    {"request_id": request.request_id, "vm_count": len(vms), "vm_profile": request.vm_profile},
                )
                self._emit_progress(
                    "external_request_arrival",
                    {"request_id": request.request_id, "vm_count": len(vms), "vm_profile": request.vm_profile},
                )

            yield self.env.timeout(1)

    def _scheduling_process(self):
        """
        Periodically invoke the scheduler to place pending VMs.
        """
        while True:
            placed_any = False
            if self.pending_vms:
                # Call the scheduling algorithm
                placements = self.scheduler(self, self.pending_vms, list(self.servers.values()))
                # placements: list of (vm, server) tuples

                for vm, server in placements:
                    if server and server.place_vm(vm):
                        self.pending_vms.remove(vm)
                        vm.mark_started(self.env.now)
                        self.running_vms[vm.vm_id] = vm
                        self.server_idle_since[server.name] = None
                        placed_any = True

                        self.metrics.log_event(
                            self.env.now,
                            "vm_placement",
                            {
                                "vm_id": vm.vm_id,
                                "server": server.name,
                                "wait_time": vm.waiting_time,
                            },
                        )
                        self._emit_progress(
                            "vm_placement",
                            {
                                "vm_id": vm.vm_id,
                                "server": server.name,
                                "wait_time": vm.waiting_time,
                            },
                        )

                        # Schedule completion
                        self.vm_processes[vm.vm_id] = self.env.process(self._vm_execution(vm))

                if not placed_any:
                    # If no currently powered server can host the pending workload, wake a candidate server.
                    self._ensure_capacity_by_waking_server(self.pending_vms[0])

            yield self.env.timeout(1)  # Schedule every 1 time unit

    def _can_server_host_vm_if_awake(self, server: PhysicalServer, vm: VirtualMachine) -> bool:
        """Check static capacity fit ignoring current power state and runtime free pools."""
        if len(server.hosted_vms) >= server.max_vms:
            return False
        if vm.requires_gpu and server.gpu_count == 0:
            return False
        return (
            server.cpu_cores >= vm.required_cpu
            and server.gpu_count >= vm.required_gpu
            and server.ram_gb >= vm.required_ram
            and server.io_bandwidth_gbps >= vm.required_io_bandwidth_gbps
            and server.io_iops_k >= vm.required_io_iops_k
        )

    def _ensure_capacity_by_waking_server(self, vm: VirtualMachine):
        """Wake a suitable powered-off server so pending VMs can be scheduled."""
        if not self.enable_server_power_states:
            return

        candidate_servers = [
            server for server in self.servers.values()
            if not server.is_powered_on
            and server.name not in self.waking_servers
            and self._can_server_host_vm_if_awake(server, vm)
        ]
        if not candidate_servers:
            return

        # Prefer the lowest idle-power server to reduce wake-up cost.
        candidate_servers.sort(key=lambda server: server.idle_power_watts)
        self.env.process(self._wake_server(candidate_servers[0]))

    def _wake_server(self, server: PhysicalServer):
        """Wake a powered-off server with delay and energy overhead."""
        self.waking_servers.add(server.name)
        self.metrics.log_event(
            self.env.now,
            "server_wakeup_start",
            {"server": server.name, "delay": self.server_wakeup_delay},
        )

        yield self.env.timeout(self.server_wakeup_delay)
        server.is_powered_on = True
        self.server_idle_since[server.name] = self.env.now
        self.waking_servers.discard(server.name)

        self.metrics.total_server_wakeups += 1
        self.metrics.total_server_power_state_energy_kwh += self.server_wakeup_energy_kwh
        self.metrics.total_energy_kwh += self.server_wakeup_energy_kwh
        self.metrics.log_event(
            self.env.now,
            "server_wakeup_complete",
            {"server": server.name},
        )
        self._emit_progress("server_wakeup_complete", {"server": server.name})

    def _vm_execution(self, vm: VirtualMachine):
        """
        Simulate VM execution and completion with migration-aware interruptions.
        """
        remaining_time = vm.service_time
        segment_start = self.env.now

        while remaining_time > 0:
            try:
                yield self.env.timeout(remaining_time)
                remaining_time = 0
            except simpy.Interrupt as interrupt:
                elapsed = self.env.now - segment_start
                remaining_time = max(0.0, remaining_time - elapsed)
                downtime = 0.0
                if isinstance(interrupt.cause, dict):
                    downtime = float(interrupt.cause.get("downtime", 0.0))
                if downtime > 0:
                    yield self.env.timeout(downtime)
                segment_start = self.env.now

        # Mark complete
        vm.mark_completed(self.env.now)
        self.running_vms.pop(vm.vm_id, None)
        self.vm_processes.pop(vm.vm_id, None)
        self.migrating_vm_ids.discard(vm.vm_id)
        self.completed_vms.append(vm)
        current_server = vm.assigned_server
        if current_server is not None:
            current_server.remove_vm(vm)

        # Track metrics
        self.metrics.total_vms_completed += 1
        if vm.sla_violated:
            self.metrics.total_sla_violations += 1

        self.metrics.log_event(
            self.env.now,
            "vm_completion",
            {
                "vm_id": vm.vm_id,
                "server": current_server.name if current_server else None,
                "turnaround_time": vm.get_turnaround_time(),
                "sla_violated": vm.sla_violated,
            },
        )
        self._emit_progress(
            "vm_completion",
            {
                "vm_id": vm.vm_id,
                "server": current_server.name if current_server else None,
                "turnaround_time": vm.get_turnaround_time(),
                "sla_violated": vm.sla_violated,
            },
        )

    def _migration_process(self):
        """Periodically evaluate running VMs and migrate when beneficial."""
        while True:
            yield self.env.timeout(self.migration_check_interval)
            self._attempt_consolidation_migrations()

    def _attempt_consolidation_migrations(self):
        """Try migrating VMs off lightly utilized servers to consolidate load."""
        source_servers = [
            server for server in self.servers.values()
            if 0 < len(server.hosted_vms)
            and server.current_utilization <= self.migration_source_max_util
        ]

        for source in source_servers:
            source_last = self.server_last_migration_time.get(source.name, float("-inf"))
            if (self.env.now - source_last) < self.server_migration_cooldown:
                continue

            candidate_vms = sorted(
                source.hosted_vms,
                key=lambda vm: (
                    vm.required_cpu + vm.required_ram + vm.required_io_iops_k + vm.required_io_bandwidth_gbps
                ),
                reverse=True,
            )
            for vm in candidate_vms:
                if vm.vm_id in self.migrating_vm_ids:
                    continue
                if vm.migration_count >= self.max_migrations_per_vm:
                    continue
                vm_last = self.vm_last_migration_time.get(vm.vm_id, float("-inf"))
                if (self.env.now - vm_last) < self.vm_migration_cooldown:
                    continue

                destination = self._find_migration_destination(vm, source)
                if destination is None:
                    continue

                self.env.process(self._migrate_vm(vm, source, destination))
                # Migrate one VM per source per cycle to avoid instability.
                break

    def _find_migration_destination(
        self,
        vm: VirtualMachine,
        source: PhysicalServer,
    ) -> Optional[PhysicalServer]:
        """Find a destination server that can host the VM with low incremental power."""
        best_server = None
        best_net_gain = float("-inf")

        for destination in self.servers.values():
            if destination.name == source.name:
                continue
            if not destination.is_powered_on:
                continue
            if not destination.can_fit_vm(vm):
                continue

            dest_last = self.server_last_migration_time.get(destination.name, float("-inf"))
            if (self.env.now - dest_last) < self.server_migration_cooldown:
                continue

            projected_util = destination.estimate_utilization_after_placement(vm)
            if projected_util > self.migration_dest_max_util:
                continue

            net_gain = self._estimate_net_migration_energy_gain_kwh(vm, source, destination)
            if net_gain < self.migration_min_net_energy_gain_kwh:
                continue

            if net_gain > best_net_gain:
                best_net_gain = net_gain
                best_server = destination

        return best_server

    def _estimate_net_migration_energy_gain_kwh(
        self,
        vm: VirtualMachine,
        source: PhysicalServer,
        destination: PhysicalServer,
    ) -> float:
        """Estimate migration net energy gain over a short planning horizon."""
        source_current_power = source.get_current_power_draw()
        dest_current_power = destination.get_current_power_draw()

        # Destination after placement
        projected_dest_util = destination.estimate_utilization_after_placement(vm)
        dest_after_power = destination.idle_power_watts + (
            (destination.peak_power_watts - destination.idle_power_watts)
            * projected_dest_util
            * destination.efficiency_factor
        )

        # Source after removal
        if len(source.hosted_vms) <= 1 and self.enable_server_power_states:
            source_after_power = 0.0
        else:
            removed_cpu = source.available_cpu + vm.required_cpu
            removed_ram = source.available_ram + vm.required_ram
            removed_gpu = source.available_gpu + vm.required_gpu
            removed_io_bw = source.available_io_bandwidth_gbps + vm.required_io_bandwidth_gbps
            removed_io_iops = source.available_io_iops_k + vm.required_io_iops_k

            cpu_util = (source.cpu_cores - removed_cpu) / source.cpu_cores if source.cpu_cores > 0 else 0.0
            ram_util = (source.ram_gb - removed_ram) / source.ram_gb if source.ram_gb > 0 else 0.0
            gpu_util = (source.gpu_count - removed_gpu) / source.gpu_count if source.gpu_count > 0 else 0.0
            io_bw_util = (
                (source.io_bandwidth_gbps - removed_io_bw) / source.io_bandwidth_gbps
                if source.io_bandwidth_gbps > 0 else 0.0
            )
            io_iops_util = (
                (source.io_iops_k - removed_io_iops) / source.io_iops_k
                if source.io_iops_k > 0 else 0.0
            )
            source_after_util = max(cpu_util, ram_util, gpu_util, io_bw_util, io_iops_util)
            source_after_power = source.idle_power_watts + (
                (source.peak_power_watts - source.idle_power_watts)
                * source_after_util
                * source.efficiency_factor
            )

        before_power = source_current_power + dest_current_power
        after_power = source_after_power + dest_after_power
        savings_watts = max(0.0, before_power - after_power)
        projected_savings_kwh = savings_watts * self.migration_energy_gain_horizon / 60000.0

        return projected_savings_kwh - self.migration_energy_overhead_kwh

    def _estimate_migration_downtime(self, vm: VirtualMachine) -> float:
        """Estimate migration pause based on VM memory size and migration bandwidth."""
        transfer_time = vm.required_ram / max(self.migration_bandwidth_gb_per_time, 0.1)
        return self.migration_base_downtime + transfer_time

    def _migrate_vm(self, vm: VirtualMachine, source: PhysicalServer, destination: PhysicalServer):
        """Execute a live migration with VM downtime and metric tracking."""
        yield self.env.timeout(0)

        if vm.vm_id in self.migrating_vm_ids:
            return
        if vm.vm_id not in self.running_vms:
            return
        if vm.assigned_server is None or vm.assigned_server.name != source.name:
            return
        if not source.is_powered_on or not destination.is_powered_on:
            return
        if not destination.can_fit_vm(vm):
            return

        self.migrating_vm_ids.add(vm.vm_id)
        vm.is_migrating = True

        downtime = self._estimate_migration_downtime(vm)
        vm_process = self.vm_processes.get(vm.vm_id)
        if vm_process is not None:
            vm_process.interrupt({"downtime": downtime})

        source.remove_vm(vm)
        placed = destination.place_vm(vm)
        if not placed:
            source.place_vm(vm)
            self.migrating_vm_ids.discard(vm.vm_id)
            vm.is_migrating = False
            self.metrics.total_failed_migrations += 1
            self.metrics.log_event(
                self.env.now,
                "vm_migration_failed",
                {
                    "vm_id": vm.vm_id,
                    "source": source.name,
                    "destination": destination.name,
                },
            )
            self._emit_progress(
                "vm_migration_failed",
                {
                    "vm_id": vm.vm_id,
                    "source": source.name,
                    "destination": destination.name,
                },
            )
            return

        vm.migration_count += 1
        vm.is_migrating = False
        self.migrating_vm_ids.discard(vm.vm_id)
        self.vm_last_migration_time[vm.vm_id] = self.env.now
        self.server_last_migration_time[source.name] = self.env.now
        self.server_last_migration_time[destination.name] = self.env.now

        self.metrics.total_migrations += 1
        self.metrics.total_migration_energy_kwh += self.migration_energy_overhead_kwh
        self.metrics.total_energy_kwh += self.migration_energy_overhead_kwh
        self.metrics.log_event(
            self.env.now,
            "vm_migration",
            {
                "vm_id": vm.vm_id,
                "source": source.name,
                "destination": destination.name,
                "downtime": downtime,
                "migration_count": vm.migration_count,
            },
        )
        self._emit_progress(
            "vm_migration",
            {
                "vm_id": vm.vm_id,
                "source": source.name,
                "destination": destination.name,
                "downtime": downtime,
            },
        )

        # Start idle timer immediately for a fully drained server so it can be powered down.
        if len(source.hosted_vms) == 0 and source.is_powered_on:
            self.server_idle_since[source.name] = self.env.now

    def _server_power_state_process(self):
        """Periodically power off servers that remain idle after consolidation."""
        while True:
            yield self.env.timeout(self.server_power_state_check_interval)

            for server in self.servers.values():
                if not server.is_powered_on or server.name in self.waking_servers:
                    continue

                if len(server.hosted_vms) > 0:
                    self.server_idle_since[server.name] = None
                    continue

                idle_since = self.server_idle_since.get(server.name)
                if idle_since is None:
                    self.server_idle_since[server.name] = self.env.now
                    continue

                if (self.env.now - idle_since) >= self.server_idle_shutdown_time:
                    server.is_powered_on = False
                    self.server_idle_since[server.name] = None
                    self.metrics.total_server_shutdowns += 1
                    self.metrics.log_event(
                        self.env.now,
                        "server_power_off",
                        {"server": server.name},
                    )
                    self._emit_progress("server_power_off", {"server": server.name})

    def _metrics_logging_process(self, log_interval: float):
        """
        Periodically log aggregate metrics.
        """
        while True:
            yield self.env.timeout(log_interval)

            # Aggregate current state
            total_power = sum(srv.get_current_power_draw() for srv in self.servers.values())
            total_util = sum(srv.current_utilization for srv in self.servers.values()) / len(
                self.servers
            )
            queue_len = len(self.pending_vms)

            self.metrics.total_energy_kwh += (total_power / 1000) * (log_interval / 60)
            self.metrics.record_time_series(self.env.now, total_power, total_util, queue_len)
            self._emit_progress(
                "metrics_tick",
                {
                    "total_power_watts": total_power,
                    "avg_utilization": total_util,
                    "queue_length": queue_len,
                    "running_vm_count": len(self.running_vms),
                },
            )

            if self.realtime_delay_seconds > 0:
                time.sleep(self.realtime_delay_seconds)

            if len(self.metrics.event_log) % 50 == 0:
                print(
                    f"Time: {self.env.now:6.1f} | Queue: {queue_len:3d} | "
                    f"Power: {total_power:6.0f}W | Utilization: {total_util:5.1%} | "
                    f"Running: {len(self.running_vms):3d}"
                )

    def _finalize_metrics(self):
        """
        Post-simulation metric computation.
        """
        for vm in self.completed_vms:
            state = vm.get_state()
            self.metrics.record_vm_metric(vm.vm_id, "waiting_time", vm.waiting_time)
            self.metrics.record_vm_metric(vm.vm_id, "turnaround_time", vm.get_turnaround_time())
            self.metrics.record_vm_metric(vm.vm_id, "sla_violated", vm.sla_violated)
            self.metrics.record_vm_metric(vm.vm_id, "migration_count", vm.migration_count)

        for server in self.servers.values():
            state = server.get_state()
            self.metrics.record_server_metric(server.name, "final_state", state)

    def get_servers(self) -> List[PhysicalServer]:
        """Return list of servers."""
        return list(self.servers.values())

    def get_pending_vms(self) -> List[VirtualMachine]:
        """Return list of pending VMs."""
        return self.pending_vms.copy()

    def get_running_vms(self) -> List[VirtualMachine]:
        """Return list of running VMs."""
        return list(self.running_vms.values())

    def get_completed_vms(self) -> List[VirtualMachine]:
        """Return list of completed VMs."""
        return self.completed_vms.copy()

    def inject_request(self, vm_profile: str, vm_count: int = 1, priority: int = 1) -> WorkloadRequest:
        """Queue an external workload request to be injected on the next polling cycle."""
        request = WorkloadRequest(
            request_id=f"external_req_{self._external_request_counter}",
            arrival_time=self.env.now,
            vm_profile=vm_profile,
            vm_count=vm_count,
            priority=priority,
        )
        self._external_request_counter += 1
        with self.external_request_lock:
            self.external_requests.append(request)
        return request

    def get_snapshot(self) -> dict:
        """Return a serializable view of current simulation state for APIs or dashboards."""
        return {
            "time": self.env.now,
            "pending_vm_count": len(self.pending_vms),
            "running_vm_count": len(self.running_vms),
            "completed_vm_count": len(self.completed_vms),
            "is_finished": self.is_finished,
            "servers": [server.get_state() for server in self.get_servers()],
            "latest_metrics": self.metrics.get_statistics(),
            "recent_events": self.metrics.event_log[-25:],
        }

    def _emit_progress(self, event_type: str, details: Optional[dict] = None):
        """Emit structured progress updates to an optional observer callback."""
        if self.progress_callback is None:
            return
        payload = {
            "event_type": event_type,
            "time": self.env.now,
            "details": details or {},
            "snapshot": self.get_snapshot(),
        }
        self.progress_callback(payload)
