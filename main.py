"""
Main entry point for the Green Cloud Data Center Simulator.
Example of running a single scenario with different schedulers.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.default_config import *
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from src.schedulers import (
    fcfs_scheduler,
    best_fit_scheduler,
    energy_aware_scheduler,
    sjf_scheduler,
    round_robin_scheduler,
    aco_scheduler,
    gwo_scheduler,
    rl_pa_scheduler,
)
from utils.metrics import MetricsCollector


def run_scenario(scheduler_func, scheduler_name: str, workload_pattern: str = "poisson"):
    """
    Run a single simulation scenario.
    
    Args:
        scheduler_func: The scheduling algorithm to use.
        scheduler_name: Display name for results.
        workload_pattern: 'poisson', 'bursty', or 'time_varying'.
    """
    # Create metrics collector
    metrics = MetricsCollector(simulation_name=f"{scheduler_name}_{workload_pattern}")

    # Create workload generator
    workload_gen = WorkloadGenerator(
        arrival_rate=WORKLOAD_ARRIVAL_RATE,
        vm_profiles=VM_PROFILES,
        service_time_mean=SERVICE_TIME_MEAN,
        service_time_std=SERVICE_TIME_STD,
        random_seed=RANDOM_SEED,
    )

    # Create simulation
    sim = CloudDataCenterSimulation(
        servers=PHYSICAL_SERVERS,
        vm_profiles=VM_PROFILES,
        workload_generator=workload_gen,
        scheduler=scheduler_func,
        max_sla_time=MAX_WAIT_TIME,
        metrics_collector=metrics,
        random_seed=RANDOM_SEED,
        enable_vm_migration=ENABLE_VM_MIGRATION,
        migration_check_interval=MIGRATION_CHECK_INTERVAL,
        migration_source_max_util=MIGRATION_SOURCE_MAX_UTIL,
        migration_dest_max_util=MIGRATION_DEST_MAX_UTIL,
        max_migrations_per_vm=MAX_MIGRATIONS_PER_VM,
        vm_migration_cooldown=VM_MIGRATION_COOLDOWN,
        server_migration_cooldown=SERVER_MIGRATION_COOLDOWN,
        migration_base_downtime=MIGRATION_BASE_DOWNTIME,
        migration_bandwidth_gb_per_time=MIGRATION_BANDWIDTH_GB_PER_TIME,
        migration_energy_overhead_kwh=MIGRATION_ENERGY_OVERHEAD_KWH,
        migration_energy_gain_horizon=MIGRATION_ENERGY_GAIN_HORIZON,
        migration_min_net_energy_gain_kwh=MIGRATION_MIN_NET_ENERGY_GAIN_KWH,
        enable_server_power_states=ENABLE_SERVER_POWER_STATES,
        server_idle_shutdown_time=SERVER_IDLE_SHUTDOWN_TIME,
        server_power_state_check_interval=SERVER_POWER_STATE_CHECK_INTERVAL,
        server_wakeup_delay=SERVER_WAKEUP_DELAY,
        server_wakeup_energy_kwh=SERVER_WAKEUP_ENERGY_KWH,
    )

    # Run
    sim.run(
        simulation_time=SIMULATION_TIME,
        workload_pattern=workload_pattern,
        log_interval=LOG_INTERVAL,
    )

    # Export results
    output_file = f"output/{scheduler_name}_{workload_pattern}_results.json"
    os.makedirs("output", exist_ok=True)
    metrics.export_to_json(output_file)
    print(f"Results exported to {output_file}\n")

    return metrics


def main():
    """
    Run multiple scheduling algorithms on the same scenario for comparison.
    """
    print("\n" + "="*70)
    print("GREEN CLOUD DATA CENTER SIMULATOR - SCHEDULER COMPARISON")
    print("="*70)

    # Define schedulers to test
    schedulers = [
        (fcfs_scheduler, "FCFS"),
        (round_robin_scheduler, "RoundRobin"),
        (best_fit_scheduler, "BestFit"),
        (energy_aware_scheduler, "EnergyAware"),
        (sjf_scheduler, "SJF"),
        (aco_scheduler, "ACO"),
        (gwo_scheduler, "GWO"),
        (rl_pa_scheduler, "RLPA"),
    ]

    workload_pattern = "poisson"  # Can also be 'bursty' or 'time_varying'

    results = {}
    for sched_func, sched_name in schedulers:
        print(f"\n{'='*70}")
        print(f"Running scenario with {sched_name} scheduler")
        print(f"{'='*70}")
        metrics = run_scenario(sched_func, sched_name, workload_pattern)
        results[sched_name] = metrics.get_statistics()

    # Print comparison table
    print("\n" + "="*70)
    print("COMPARISON ACROSS SCHEDULERS")
    print("="*70)

    metrics_keys = [
        "total_vms_completed",
        "total_migrations",
        "total_server_shutdowns",
        "sla_violation_rate",
        "avg_waiting_time",
        "avg_turnaround_time",
        "total_energy_kwh",
        "avg_power_watts",
        "avg_utilization",
        "carbon_footprint_kg_baseline",
        "carbon_footprint_kg_renewable_avg",
    ]

    # Print header
    print(f"{'Metric':<25}", end="")
    for sched_name in results.keys():
        print(f"{sched_name:>15}", end="")
    print()
    print("-" * (25 + 15 * len(results)))

    # Print rows
    for key in metrics_keys:
        print(f"{key:<25}", end="")
        for sched_name, stats in results.items():
            value = stats.get(key, 0)
            if isinstance(value, float):
                if "rate" in key or "utilization" in key:
                    print(f"{value:>14.1%}", end="")
                else:
                    print(f"{value:>15.2f}", end="")
            else:
                print(f"{value:>15}", end="")
        print()

    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    main()
