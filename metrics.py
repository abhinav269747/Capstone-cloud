"""
Metrics tracking and logging for simulation results.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
import json
from datetime import datetime


@dataclass
class MetricsCollector:
    """
    Collects and aggregates metrics throughout simulation.
    """
    simulation_name: str
    start_time: datetime = field(default_factory=datetime.now)

    # Event log
    event_log: List[dict] = field(default_factory=list)

    # Per-VM metrics
    vm_metrics: Dict[str, dict] = field(default_factory=dict)

    # Aggregate metrics
    total_energy_kwh: float = 0.0
    total_vms_completed: int = 0
    total_vms_failed: int = 0
    total_sla_violations: int = 0
    total_vms_in_queue: int = 0
    total_migrations: int = 0
    total_failed_migrations: int = 0
    total_migration_energy_kwh: float = 0.0
    total_server_wakeups: int = 0
    total_server_shutdowns: int = 0
    total_server_power_state_energy_kwh: float = 0.0

    # Server metrics
    server_metrics: Dict[str, dict] = field(default_factory=dict)

    # Time-series data
    time_steps: List[float] = field(default_factory=list)
    power_history: List[float] = field(default_factory=list)
    utilization_history: List[float] = field(default_factory=list)
    queue_length_history: List[int] = field(default_factory=list)

    def log_event(self, time: float, event_type: str, details: dict):
        """Log a simulation event."""
        event = {
            "time": time,
            "event_type": event_type,
            **details,
        }
        self.event_log.append(event)

    def record_vm_metric(self, vm_id: str, metric_name: str, value):
        """Record a per-VM metric."""
        if vm_id not in self.vm_metrics:
            self.vm_metrics[vm_id] = {}
        self.vm_metrics[vm_id][metric_name] = value

    def record_server_metric(self, server_name: str, metric_name: str, value):
        """Record a per-server metric."""
        if server_name not in self.server_metrics:
            self.server_metrics[server_name] = {}
        self.server_metrics[server_name][metric_name] = value

    def record_time_series(self, time: float, power: float, utilization: float, queue_length: int):
        """Record metrics at a time point."""
        self.time_steps.append(time)
        self.power_history.append(power)
        self.utilization_history.append(utilization)
        self.queue_length_history.append(queue_length)

    def get_statistics(self) -> dict:
        """
        Compute aggregate statistics.
        """
        waiting_times = [m.get("waiting_time") for m in self.vm_metrics.values() if m.get("waiting_time")]
        turnaround_times = [m.get("turnaround_time") for m in self.vm_metrics.values() if m.get("turnaround_time")]
        
        avg_waiting = sum(waiting_times) / len(waiting_times) if waiting_times else 0
        avg_turnaround = sum(turnaround_times) / len(turnaround_times) if turnaround_times else 0
        max_waiting = max(waiting_times) if waiting_times else 0
        max_turnaround = max(turnaround_times) if turnaround_times else 0

        avg_power = sum(self.power_history) / len(self.power_history) if self.power_history else 0
        peak_power = max(self.power_history) if self.power_history else 0
        avg_utilization = sum(self.utilization_history) / len(self.utilization_history) if self.utilization_history else 0

        sla_violation_rate = (
            self.total_sla_violations / self.total_vms_completed
            if self.total_vms_completed > 0
            else 0
        )

        return {
            "total_vms_completed": self.total_vms_completed,
            "total_vms_failed": self.total_vms_failed,
            "total_sla_violations": self.total_sla_violations,
            "total_migrations": self.total_migrations,
            "total_failed_migrations": self.total_failed_migrations,
            "total_server_wakeups": self.total_server_wakeups,
            "total_server_shutdowns": self.total_server_shutdowns,
            "sla_violation_rate": sla_violation_rate,
            "avg_waiting_time": avg_waiting,
            "max_waiting_time": max_waiting,
            "avg_turnaround_time": avg_turnaround,
            "max_turnaround_time": max_turnaround,
            "total_energy_kwh": self.total_energy_kwh,
            "total_migration_energy_kwh": self.total_migration_energy_kwh,
            "total_server_power_state_energy_kwh": self.total_server_power_state_energy_kwh,
            "avg_power_watts": avg_power,
            "peak_power_watts": peak_power,
            "avg_utilization": avg_utilization,
            "carbon_footprint_kg_baseline": self.carbon_footprint_kg(pue=1.3, carbon_intensity_g_per_kwh=386.0, renewable_fraction=0.0),
            "carbon_footprint_kg_renewable_avg": self.carbon_footprint_kg(pue=1.3, carbon_intensity_g_per_kwh=386.0, renewable_fraction=0.3),
        }

    def carbon_footprint_kg(
        self,
        pue: float = 1.3,
        carbon_intensity_g_per_kwh: float = 386.0,
        renewable_fraction: float = 0.0,
    ) -> float:
        """
        Calculate carbon footprint in kg CO2e.

        Args:
            pue: Power Usage Effectiveness (1.3 = 30% overhead for cooling, etc.)
            carbon_intensity_g_per_kwh: Grid carbon intensity (gCO2/kWh).
                # Default: 386 g/kWh = US national average (EPA eGRID 2023)
            renewable_fraction: Fraction of energy from renewables (0.0 - 1.0).

        Returns:
            CO2e in kg (rounded to 2 decimals).
        """
        grid_fraction = max(0.0, 1.0 - renewable_fraction)
        co2_kg = (
            self.total_energy_kwh
            * pue
            * (carbon_intensity_g_per_kwh / 1000.0)
            * grid_fraction
        )
        return co2_kg

    def export_to_json(self, filepath: str):
        """Export metrics to JSON file."""
        data = {
            "simulation_name": self.simulation_name,
            "start_time": str(self.start_time),
            "statistics": self.get_statistics(),
            "event_log": self.event_log[:100],  # First 100 events for brevity
            "vm_metrics_count": len(self.vm_metrics),
            "server_metrics": self.server_metrics,
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def print_summary(self):
        """Print a human-readable summary."""
        stats = self.get_statistics()
        print("\n" + "=" * 60)
        print(f"SIMULATION SUMMARY: {self.simulation_name}")
        print("=" * 60)
        print(f"VMs Completed:        {stats['total_vms_completed']}")
        print(f"VMs Failed:          {stats['total_vms_failed']}")
        print(f"VM Migrations:       {stats['total_migrations']} (failed: {stats['total_failed_migrations']})")
        print(f"Server Wakeups:      {stats['total_server_wakeups']}")
        print(f"Server Shutdowns:    {stats['total_server_shutdowns']}")
        print(f"SLA Violations:      {stats['total_sla_violations']} ({stats['sla_violation_rate']:.1%})")
        print(f"Avg Waiting Time:    {stats['avg_waiting_time']:.2f} units")
        print(f"Avg Turnaround Time: {stats['avg_turnaround_time']:.2f} units")
        print(f"Total Energy:        {stats['total_energy_kwh']:.2f} kWh")
        print(f"Migration Energy:    {stats['total_migration_energy_kwh']:.2f} kWh")
        print(f"Wakeup Energy:       {stats['total_server_power_state_energy_kwh']:.2f} kWh")
        print(f"Avg Power:           {stats['avg_power_watts']:.0f} W")
        print(f"Peak Power:          {stats['peak_power_watts']:.0f} W")
        print(f"Avg Utilization:     {stats['avg_utilization']:.1%}")
        print(f"Carbon (grid-only):  {stats['carbon_footprint_kg_baseline']:.2f} kg CO2e")
        print(f"Carbon (30% renew):  {stats['carbon_footprint_kg_renewable_avg']:.2f} kg CO2e")
        print("=" * 60 + "\n")
