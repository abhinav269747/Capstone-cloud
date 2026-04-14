"""
Core data models for the cloud data center simulator.
Represents physical servers, virtual machines, and resource requests.
"""

from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum


class VMState(Enum):
    """Lifecycle states for a virtual machine."""
    PENDING = "pending"  # Waiting in queue
    RUNNING = "running"   # Executing
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PhysicalServer:
    """
    Represents a physical machine in the data center.
    Each server has unique compute, memory, GPU, and energy characteristics.
    """
    name: str
    cpu_cores: int
    gpu_count: int
    ram_gb: int
    max_vms: int
    idle_power_watts: float
    peak_power_watts: float
    efficiency_factor: float  # Ratio of efficiency at partial load
    io_bandwidth_gbps: float = 10.0
    io_iops_k: float = 100.0

    # Runtime state
    available_cpu: int = field(init=False)
    available_gpu: int = field(init=False)
    available_ram: int = field(init=False)
    available_io_bandwidth_gbps: float = field(init=False)
    available_io_iops_k: float = field(init=False)
    hosted_vms: List['VirtualMachine'] = field(default_factory=list, init=False)
    is_powered_on: bool = field(default=True, init=False)
    current_utilization: float = field(default=0.0, init=False)  # 0.0 to 1.0

    def __post_init__(self):
        """Initialize runtime state."""
        self.available_cpu = self.cpu_cores
        self.available_gpu = self.gpu_count
        self.available_ram = self.ram_gb
        self.available_io_bandwidth_gbps = self.io_bandwidth_gbps
        self.available_io_iops_k = self.io_iops_k

    def can_fit_vm(self, vm: 'VirtualMachine') -> bool:
        """Check if a VM can be placed on this server."""
        if not self.is_powered_on:
            return False
        if len(self.hosted_vms) >= self.max_vms:
            return False
        cpu_ok = self.available_cpu >= vm.required_cpu
        gpu_ok = self.available_gpu >= vm.required_gpu
        ram_ok = self.available_ram >= vm.required_ram
        io_bw_ok = self.available_io_bandwidth_gbps >= vm.required_io_bandwidth_gbps
        io_iops_ok = self.available_io_iops_k >= vm.required_io_iops_k
        if vm.requires_gpu and self.gpu_count == 0:
            return False
        return cpu_ok and gpu_ok and ram_ok and io_bw_ok and io_iops_ok

    def place_vm(self, vm: 'VirtualMachine') -> bool:
        """Place a VM on this server. Returns True if successful."""
        if not self.can_fit_vm(vm):
            return False
        self.available_cpu -= vm.required_cpu
        self.available_gpu -= vm.required_gpu
        self.available_ram -= vm.required_ram
        self.available_io_bandwidth_gbps -= vm.required_io_bandwidth_gbps
        self.available_io_iops_k -= vm.required_io_iops_k
        self.hosted_vms.append(vm)
        vm.assigned_server = self
        self._update_utilization()
        return True

    def remove_vm(self, vm: 'VirtualMachine') -> bool:
        """Remove a VM from this server (e.g., after completion or migration)."""
        if vm not in self.hosted_vms:
            return False
        self.available_cpu += vm.required_cpu
        self.available_gpu += vm.required_gpu
        self.available_ram += vm.required_ram
        self.available_io_bandwidth_gbps += vm.required_io_bandwidth_gbps
        self.available_io_iops_k += vm.required_io_iops_k
        self.hosted_vms.remove(vm)
        vm.assigned_server = None
        self._update_utilization()
        return True

    def get_current_power_draw(self) -> float:
        """
        Calculate current power draw based on utilization.
        Uses linear interpolation between idle and peak.
        """
        if not self.is_powered_on:
            return 0.0
        power_range = self.peak_power_watts - self.idle_power_watts
        # Efficiency factor reduces power draw at partial load (wastes less power at low util)
        adjusted_util = self.current_utilization * self.efficiency_factor
        return self.idle_power_watts + (power_range * adjusted_util)

    def _update_utilization(self):
        """Recalculate server utilization across compute and I/O dimensions."""
        cpu_util = (self.cpu_cores - self.available_cpu) / self.cpu_cores if self.cpu_cores > 0 else 0.0
        ram_util = (self.ram_gb - self.available_ram) / self.ram_gb if self.ram_gb > 0 else 0.0
        gpu_util = (self.gpu_count - self.available_gpu) / self.gpu_count if self.gpu_count > 0 else 0.0
        io_bw_util = (
            (self.io_bandwidth_gbps - self.available_io_bandwidth_gbps) / self.io_bandwidth_gbps
            if self.io_bandwidth_gbps > 0 else 0.0
        )
        io_iops_util = (
            (self.io_iops_k - self.available_io_iops_k) / self.io_iops_k
            if self.io_iops_k > 0 else 0.0
        )

        self.current_utilization = max(cpu_util, ram_util, gpu_util, io_bw_util, io_iops_util)

    def get_state(self) -> dict:
        """Return server state snapshot."""
        return {
            "name": self.name,
            "cpu_used": self.cpu_cores - self.available_cpu,
            "cpu_total": self.cpu_cores,
            "gpu_used": self.gpu_count - self.available_gpu,
            "gpu_total": self.gpu_count,
            "ram_used": self.ram_gb - self.available_ram,
            "ram_total": self.ram_gb,
            "io_bandwidth_used_gbps": self.io_bandwidth_gbps - self.available_io_bandwidth_gbps,
            "io_bandwidth_total_gbps": self.io_bandwidth_gbps,
            "io_iops_used_k": self.io_iops_k - self.available_io_iops_k,
            "io_iops_total_k": self.io_iops_k,
            "vm_count": len(self.hosted_vms),
            "utilization": self.current_utilization,
            "power_watts": self.get_current_power_draw(),
            "is_powered_on": self.is_powered_on,
        }

    def estimate_utilization_after_placement(self, vm: 'VirtualMachine') -> float:
        """Estimate utilization if the VM is placed, without mutating server state."""
        cpu_util = (self.cpu_cores - (self.available_cpu - vm.required_cpu)) / self.cpu_cores if self.cpu_cores > 0 else 0.0
        ram_util = (self.ram_gb - (self.available_ram - vm.required_ram)) / self.ram_gb if self.ram_gb > 0 else 0.0
        gpu_util = (
            (self.gpu_count - (self.available_gpu - vm.required_gpu)) / self.gpu_count
            if self.gpu_count > 0 else 0.0
        )
        io_bw_util = (
            (self.io_bandwidth_gbps - (self.available_io_bandwidth_gbps - vm.required_io_bandwidth_gbps)) / self.io_bandwidth_gbps
            if self.io_bandwidth_gbps > 0 else 0.0
        )
        io_iops_util = (
            (self.io_iops_k - (self.available_io_iops_k - vm.required_io_iops_k)) / self.io_iops_k
            if self.io_iops_k > 0 else 0.0
        )

        return max(cpu_util, ram_util, gpu_util, io_bw_util, io_iops_util)


@dataclass
class VirtualMachine:
    """
    Represents a virtual machine or job request.
    """
    vm_id: str
    required_cpu: int
    required_gpu: int
    required_ram: int
    requires_gpu: bool  # Flag if GPU is mandatory
    service_time: float  # How long the job runs (in simulation time units)
    arrival_time: float
    required_io_bandwidth_gbps: float = 0.0
    required_io_iops_k: float = 0.0
    priority: int = 1  # Higher = more important (for QoS)
    sla_deadline: Optional[float] = None  # Time by which job should complete

    # Runtime state
    state: VMState = field(default=VMState.PENDING, init=False)
    assigned_server: Optional[PhysicalServer] = field(default=None, init=False)
    start_time: Optional[float] = field(default=None, init=False)
    completion_time: Optional[float] = field(default=None, init=False)
    waiting_time: Optional[float] = field(default=None, init=False)
    sla_violated: bool = field(default=False, init=False)
    migration_count: int = field(default=0, init=False)
    is_migrating: bool = field(default=False, init=False)

    def mark_started(self, current_time: float):
        """Mark VM as started."""
        self.state = VMState.RUNNING
        self.start_time = current_time
        self.waiting_time = current_time - self.arrival_time

    def mark_completed(self, current_time: float):
        """Mark VM as completed."""
        self.state = VMState.COMPLETED
        self.completion_time = current_time
        if self.sla_deadline and current_time > self.sla_deadline:
            self.sla_violated = True

    def get_turnaround_time(self) -> Optional[float]:
        """Get total time from arrival to completion."""
        if self.completion_time is None:
            return None
        return self.completion_time - self.arrival_time

    def get_state(self) -> dict:
        """Return VM state snapshot."""
        return {
            "vm_id": self.vm_id,
            "state": self.state.value,
            "arrival_time": self.arrival_time,
            "start_time": self.start_time,
            "completion_time": self.completion_time,
            "waiting_time": self.waiting_time,
            "turnaround_time": self.get_turnaround_time(),
            "assigned_server": self.assigned_server.name if self.assigned_server else None,
            "sla_violated": self.sla_violated,
            "migration_count": self.migration_count,
        }


@dataclass
class WorkloadRequest:
    """
    Represents an incoming workload request (may create one or more VMs).
    """
    request_id: str
    arrival_time: float
    vm_profile: str  # Reference to a profile in VM_PROFILES
    vm_count: int = 1  # Can request multiple VMs
    priority: int = 1

    # Populated during creation
    created_vms: List[VirtualMachine] = field(default_factory=list, init=False)


@dataclass
class SimulationEvent:
    """
    Structured event log entry for metrics and visualization.
    """
    time: float
    event_type: str  # 'arrival', 'placement', 'completion', 'migration', etc.
    request_id: Optional[str] = None
    vm_id: Optional[str] = None
    server_name: Optional[str] = None
    details: dict = field(default_factory=dict)  # Extra context
