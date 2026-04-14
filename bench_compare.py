"""Quick head-to-head benchmark of RLPA vs main competitors."""
from config.default_config import *
from src.schedulers import (
    rl_pa_scheduler, aco_scheduler, energy_aware_scheduler,
    gwo_scheduler, best_fit_scheduler, fcfs_scheduler,
    round_robin_scheduler, sjf_scheduler,
)
from src.simulation_engine import CloudDataCenterSimulation
from src.workload_generator import WorkloadGenerator
from utils.metrics import MetricsCollector


def run_algo(name, scheduler_fn, seed=42):
    m = MetricsCollector(simulation_name=name)
    wg = WorkloadGenerator(
        arrival_rate=WORKLOAD_ARRIVAL_RATE, vm_profiles=VM_PROFILES,
        service_time_mean=SERVICE_TIME_MEAN, service_time_std=SERVICE_TIME_STD,
        random_seed=seed,
    )
    sim = CloudDataCenterSimulation(
        servers=PHYSICAL_SERVERS, vm_profiles=VM_PROFILES, workload_generator=wg,
        scheduler=scheduler_fn, max_sla_time=MAX_WAIT_TIME, metrics_collector=m,
        random_seed=seed,
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
    sim.run(simulation_time=SIMULATION_TIME, workload_pattern="poisson", log_interval=50000)
    return m.get_statistics()


algos = {
    "RLPA": rl_pa_scheduler,
    "ACO": aco_scheduler,
    "GWO": gwo_scheduler,
    "EnergyAware": energy_aware_scheduler,
    "BestFit": best_fit_scheduler,
    "FCFS": fcfs_scheduler,
    "RoundRobin": round_robin_scheduler,
    "SJF": sjf_scheduler,
}

results = {}
for name, fn in algos.items():
    print(f"Running {name}...")
    results[name] = run_algo(name, fn)

keys = [
    "total_energy_kwh",
    "carbon_footprint_kg_baseline",
    "avg_waiting_time",
    "sla_violation_rate",
    "avg_power_watts",
    "avg_utilization",
    "total_vms_completed",
    "peak_power_watts",
    "total_migrations",
]

print("\n\n" + "=" * 130)
print("COMPARISON TABLE")
print("=" * 130)
header = f"{'Metric':<32}" + "".join(f"{n:>12}" for n in algos.keys())
print(header)
print("-" * len(header))
for k in keys:
    row = f"{k:<32}"
    for n in algos.keys():
        v = results[n].get(k, 0)
        row += f"{v:>12.3f}"
    print(row)

lower_better = {"total_energy_kwh", "carbon_footprint_kg_baseline", "avg_waiting_time",
                "sla_violation_rate", "avg_power_watts", "peak_power_watts", "total_migrations"}
higher_better = {"avg_utilization", "total_vms_completed"}

print("\n=== WINNERS (lower=better for cost metrics, higher=better for utilization/throughput) ===")
rlpa_wins = 0
for k in keys:
    vals = {n: results[n].get(k, 0) for n in algos.keys()}
    if k in lower_better:
        winner = min(vals, key=vals.get)
    elif k in higher_better:
        winner = max(vals, key=vals.get)
    else:
        winner = "-"
    marker = " <<<" if winner == "RLPA" else ""
    if winner == "RLPA":
        rlpa_wins += 1
    print(f"  {k:<32} -> {winner} ({vals.get(winner, 0):.3f}){marker}")

print(f"\nRLPA wins: {rlpa_wins}/{len(keys)}")
